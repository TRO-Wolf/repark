use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Builder, ListArray, new_null_array};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{
    Column, DFSchema, Result, ScalarValue, TableReference, UnnestOptions, exec_err, plan_err,
};
use datafusion::functions_nested::expr_fn as nested_fn;
use datafusion::logical_expr::expr::{Cast, ScalarFunction};
use datafusion::logical_expr::expr_fn::when;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::lit;

mod json_tuple;

pub(crate) const GENERATOR_ALIAS_UDF: &str = "__repark_gen_alias";
const GENERATOR_ORDINALITY_UDF: &str = "__repark_gen_ordinality";
const GENERATOR_FIELD_UDF: &str = "__repark_gen_field";

pub(crate) const GENERATOR_FIELD: &str = "__repark_gen_out";
const GENERATOR_POS_FIELD: &str = "__repark_gen_pos";
const GENERATOR_COL_FIELD: &str = "__repark_gen_col";
const GENERATOR_KEY_FIELD: &str = "__repark_gen_key";
const GENERATOR_VALUE_FIELD: &str = "__repark_gen_value";
const EXPLODE_TEMP_PREFIX: &str = "__repark_arr_";
const GENERATOR_TEMP_NAMES: &[&str] = &[
    "__repark_gen_out",
    "__repark_gen_pos",
    "__repark_gen_col",
    "__repark_gen_key",
    "__repark_gen_value",
];

#[derive(Debug)]
struct GeneratorPlaceholder {
    name: &'static str,
    signature: Signature,
}

impl GeneratorPlaceholder {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl PartialEq for GeneratorPlaceholder {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for GeneratorPlaceholder {}

impl Hash for GeneratorPlaceholder {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for GeneratorPlaceholder {
    fn name(&self) -> &'static str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types.first().cloned().unwrap_or(DataType::Null))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        exec_err!(
            "[UNSUPPORTED_GENERATOR] '{}' is a generator; it must sit alone in the select list",
            self.name
        )
    }
}

#[derive(Debug)]
struct GeneratorOrdinality {
    signature: Signature,
}

impl GeneratorOrdinality {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for GeneratorOrdinality {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for GeneratorOrdinality {}

impl Hash for GeneratorOrdinality {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl ScalarUDFImpl for GeneratorOrdinality {
    fn name(&self) -> &'static str {
        GENERATOR_ORDINALITY_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Int32,
            false,
        ))))
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_some_and(|field| field.is_nullable());
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&[])?,
            nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some(first) = args.args.into_iter().next() else {
            return exec_err!("'{GENERATOR_ORDINALITY_UDF}' requires one list argument");
        };
        match first {
            ColumnarValue::Array(array) => {
                let Some(list) = array.as_any().downcast_ref::<ListArray>() else {
                    return exec_err!("'{GENERATOR_ORDINALITY_UDF}' requires a list argument");
                };
                Ok(ColumnarValue::Array(Arc::new(ordinality(list))))
            }
            ColumnarValue::Scalar(ScalarValue::List(list)) => Ok(ColumnarValue::Scalar(
                ScalarValue::List(Arc::new(ordinality(list.as_ref()))),
            )),
            other @ ColumnarValue::Scalar(_) => {
                exec_err!("'{GENERATOR_ORDINALITY_UDF}' received a non-list argument: {other:?}")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct GeneratorField {
    signature: Signature,
}

impl GeneratorField {
    pub(crate) fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl PartialEq for GeneratorField {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for GeneratorField {}

impl Hash for GeneratorField {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

fn field_name_literal<'a>(
    args: &'a datafusion::logical_expr::ReturnFieldArgs<'a>,
) -> Result<&'a str> {
    if let Some(Some(
        ScalarValue::Utf8(Some(name))
        | ScalarValue::LargeUtf8(Some(name))
        | ScalarValue::Utf8View(Some(name)),
    )) = args.scalar_arguments.get(1)
    {
        return Ok(name.as_str());
    }
    plan_err!("'{GENERATOR_FIELD_UDF}' requires a literal field name")
}

fn struct_field<'a>(data_type: &'a DataType, name: &str) -> Result<&'a FieldRef> {
    let DataType::Struct(fields) = data_type else {
        return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument, got {data_type}");
    };
    let Some(index) = fields.iter().position(|field| field.name() == name) else {
        return exec_err!("'{GENERATOR_FIELD_UDF}' found no field named '{name}'");
    };
    Ok(&fields[index])
}

impl ScalarUDFImpl for GeneratorField {
    fn name(&self) -> &'static str {
        GENERATOR_FIELD_UDF
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let name = field_name_literal(&args)?;
        let Some(base) = args.arg_fields.first() else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument");
        };
        let field = struct_field(base.data_type(), name)?;
        Ok(Arc::new(Field::new(name, field.data_type().clone(), true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut iter = args.args.into_iter();
        let (Some(base), Some(name_arg)) = (iter.next(), iter.next()) else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct and a field name");
        };
        let ColumnarValue::Scalar(name_scalar) = name_arg else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a literal field name");
        };
        let (ScalarValue::Utf8(Some(name))
        | ScalarValue::LargeUtf8(Some(name))
        | ScalarValue::Utf8View(Some(name))) = name_scalar
        else {
            return exec_err!("'{GENERATOR_FIELD_UDF}' requires a literal field name");
        };
        let mask = |array: &Arc<dyn Array>| -> Result<datafusion::arrow::array::ArrayRef> {
            let Some(structs) = array
                .as_any()
                .downcast_ref::<datafusion::arrow::array::StructArray>()
            else {
                return exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument");
            };
            let Some(child) = structs.column_by_name(name.as_str()) else {
                return exec_err!("'{GENERATOR_FIELD_UDF}' found no field named '{name}'");
            };
            let combined =
                datafusion::arrow::buffer::NullBuffer::union(structs.nulls(), child.nulls());
            let data = child.to_data().into_builder().nulls(combined).build()?;
            Ok(datafusion::arrow::array::make_array(data))
        };
        match base {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(mask(&array)?)),
            ColumnarValue::Scalar(ScalarValue::Struct(structs)) => {
                let dyn_array: Arc<dyn Array> = structs;
                let masked = mask(&dyn_array)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    masked.as_ref(),
                    0,
                )?))
            }
            other @ ColumnarValue::Scalar(_) => {
                exec_err!("'{GENERATOR_FIELD_UDF}' requires a struct argument: {other:?}")
            }
        }
    }
}

fn ordinality(list: &ListArray) -> ListArray {
    let mut values = Int32Builder::with_capacity(list.values().len());
    let mut offsets: Vec<i32> = Vec::with_capacity(list.len() + 1);
    let mut running: i32 = 0;
    offsets.push(running);
    for window in list.offsets().windows(2) {
        let length = window[1] - window[0];
        for position in 0..length {
            values.append_value(position);
        }
        running += length;
        offsets.push(running);
    }
    ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values.finish()),
        list.nulls().cloned(),
    )
}

#[must_use]
pub fn generator_udf(name: &'static str) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(GeneratorPlaceholder::new(name)))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        generator_udf("posexplode"),
        generator_udf("posexplode_outer"),
        generator_udf("inline"),
        generator_udf("inline_outer"),
        generator_udf("json_tuple"),
        generator_udf(GENERATOR_ALIAS_UDF),
    ]
}

#[derive(Clone, Copy, Debug)]
enum GeneratorKind {
    PosExplode { outer: bool },
    Inline { outer: bool },
    JsonTuple,
}

impl GeneratorKind {
    fn label(self) -> &'static str {
        match self {
            Self::PosExplode { outer: false } => "posexplode",
            Self::PosExplode { outer: true } => "posexplode_outer",
            Self::Inline { outer: false } => "inline",
            Self::Inline { outer: true } => "inline_outer",
            Self::JsonTuple => "json_tuple",
        }
    }

    fn outer(self) -> bool {
        match self {
            Self::PosExplode { outer } | Self::Inline { outer } => outer,
            Self::JsonTuple => false,
        }
    }
}

fn generator_kind(name: &str) -> Option<GeneratorKind> {
    match name {
        "posexplode" => Some(GeneratorKind::PosExplode { outer: false }),
        "posexplode_outer" => Some(GeneratorKind::PosExplode { outer: true }),
        "inline" => Some(GeneratorKind::Inline { outer: false }),
        "inline_outer" => Some(GeneratorKind::Inline { outer: true }),
        "json_tuple" => Some(GeneratorKind::JsonTuple),
        _ => None,
    }
}

type GeneratorSite = (GeneratorKind, Vec<Expr>, Option<Vec<String>>);

#[derive(Debug)]
pub(crate) struct OutputSpec {
    pub(crate) member: String,
    pub(crate) name: String,
    pub(crate) nullable: bool,
}

#[derive(Debug, Default)]
pub struct GeneratorRewrite;

impl AnalyzerRule for GeneratorRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    fn name(&self) -> &'static str {
        "generator_rewrite"
    }
}

fn is_generator_call(function: &ScalarFunction) -> bool {
    generator_kind(function.func.name()).is_some()
}

fn contains_generator(expr: &Expr) -> bool {
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::ScalarFunction(function)
                if is_generator_call(function) || function.func.name() == GENERATOR_ALIAS_UDF
        ))
    })
    .unwrap_or(false)
}

fn contains_aggregate(expr: &Expr) -> bool {
    expr.exists(|node| Ok(matches!(node, Expr::AggregateFunction(_))))
        .unwrap_or(false)
}

fn contains_named_call(expr: &Expr, name: &str) -> bool {
    expr.exists(|node| {
        Ok(matches!(
            node,
            Expr::ScalarFunction(function) if function.func.name() == name
        ))
    })
    .unwrap_or(false)
}

fn is_explode_temp_name(name: &str) -> bool {
    name.strip_prefix(EXPLODE_TEMP_PREFIX)
        .is_some_and(|tail| tail.len() == 32 && tail.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn peel_generator(expr: &Expr) -> Result<Option<GeneratorSite>> {
    match expr {
        Expr::Alias(alias) => {
            let Some((kind, arg, inner_names)) = peel_generator(alias.expr.as_ref())? else {
                return Ok(None);
            };
            let restored = matches!(
                alias.expr.as_ref(),
                Expr::ScalarFunction(function)
                    if alias
                        .name
                        .starts_with(&format!("{}(", function.func.name()))
            );
            if restored {
                return Ok(Some((kind, arg, inner_names)));
            }
            if inner_names.is_some() {
                return plan_err!(
                    "[UNSUPPORTED_GENERATOR] a generator output does not support nested aliasing"
                );
            }
            Ok(Some((kind, arg, Some(vec![alias.name.clone()]))))
        }
        Expr::ScalarFunction(function) if function.func.name() == GENERATOR_ALIAS_UDF => {
            let Some(inner) = function.args.first() else {
                return plan_err!(
                    "'{GENERATOR_ALIAS_UDF}' requires a generator call and output names"
                );
            };
            let mut names = Vec::with_capacity(function.args.len() - 1);
            for name_expr in &function.args[1..] {
                match name_expr {
                    Expr::Literal(
                        ScalarValue::Utf8(Some(name))
                        | ScalarValue::LargeUtf8(Some(name))
                        | ScalarValue::Utf8View(Some(name)),
                        _,
                    ) => names.push(name.clone()),
                    _ => {
                        return plan_err!(
                            "'{GENERATOR_ALIAS_UDF}' output names must be string literals"
                        );
                    }
                }
            }
            let Some((kind, arg, _)) = peel_generator(inner)? else {
                return plan_err!("'{GENERATOR_ALIAS_UDF}' wraps a non-generator expression");
            };
            Ok(Some((kind, arg, Some(names))))
        }
        Expr::ScalarFunction(function) => {
            let Some(kind) = generator_kind(function.func.name()) else {
                return Ok(None);
            };
            if matches!(kind, GeneratorKind::JsonTuple) {
                if function.args.len() < 2 {
                    return Err(crate::json::tuple::wrong_num_args(function.args.len()));
                }
                return Ok(Some((kind, function.args.clone(), None)));
            }
            if function.args.len() != 1 {
                return plan_err!(
                    "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] '{}' expects 1 argument, got {}",
                    function.func.name(),
                    function.args.len()
                );
            }
            Ok(Some((kind, vec![function.args[0].clone()], None)))
        }
        _ => Ok(None),
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let mut site: Option<(usize, GeneratorSite)> = None;
    for (index, expr) in projection.expr.iter().enumerate() {
        match peel_generator(expr)? {
            Some(found) => {
                if site.is_some() {
                    return plan_err!("Only one generator allowed per select list");
                }
                site = Some((index, found));
            }
            None => {
                if contains_generator(expr) {
                    return plan_err!(
                        "[UNSUPPORTED_GENERATOR] The generator expression cannot be nested \
                         inside another expression"
                    );
                }
            }
        }
    }
    let Some((site_index, (kind, args, alias_names))) = site else {
        return Ok(Transformed::no(plan));
    };
    if args.iter().any(contains_generator) {
        return plan_err!(
            "[UNSUPPORTED_GENERATOR] The generator argument cannot contain another generator"
        );
    }
    let input_schema = projection.input.schema();
    let aggr_names: std::collections::HashSet<String> =
        if let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() {
            aggregate
                .aggr_expr
                .iter()
                .map(|expr| expr.schema_name().to_string())
                .collect()
        } else {
            std::collections::HashSet::new()
        };
    let references_aggr = |expr: &Expr| -> bool {
        contains_aggregate(expr)
            || expr
                .exists(|node| {
                    Ok(matches!(
                        node,
                        Expr::Column(column) if aggr_names.contains(&column.name)
                    ))
                })
                .unwrap_or(false)
    };
    if args.iter().any(|arg| references_aggr(arg)) {
        return plan_err!(
            "[MISSING_GROUP_BY] The query does not include a GROUP BY clause. Add GROUP BY or \
             turn it into the window functions using OVER clauses."
        );
    }
    for (index, expr) in projection.expr.iter().enumerate() {
        if index == site_index {
            continue;
        }
        if references_aggr(expr) {
            return plan_err!(
                "[MISSING_GROUP_BY] The query does not include a GROUP BY clause. Add GROUP BY \
                 or turn it into the window functions using OVER clauses."
            );
        }
        if contains_named_call(expr, "stack") {
            return plan_err!("Only one generator allowed per select list");
        }
        let Ok((_, field)) = expr.to_field(input_schema.as_ref()) else {
            continue;
        };
        if is_explode_temp_name(field.name()) {
            return plan_err!("Only one generator allowed per select list");
        }
        if GENERATOR_TEMP_NAMES.contains(&field.name().as_str()) {
            return plan_err!(
                "generator output field name '{}' is reserved for repark internals",
                field.name()
            );
        }
    }
    Ok(Transformed::yes(rewrite_projection(
        projection,
        site_index,
        kind,
        &args,
        alias_names.as_deref(),
    )?))
}

fn list_element(data_type: &DataType) -> Option<&FieldRef> {
    match data_type {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => Some(field),
        _ => None,
    }
}

fn unexpected_input(
    name: &str,
    got: &DataType,
    requirement: &str,
) -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(format!(
        "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \"{name}(<expr>)\" due to \
         data type mismatch: The first parameter requires the {requirement} type, however the \
         argument has the type \"{got}\"."
    ))
}

fn one_element_null_list(list_type: &DataType) -> Result<Expr> {
    match list_type {
        DataType::List(field) => {
            let values = new_null_array(field.data_type(), 1);
            let array = ListArray::new(
                Arc::clone(field),
                OffsetBuffer::from_lengths([1_usize]),
                values,
                None,
            );
            Ok(lit(ScalarValue::List(Arc::new(array))))
        }
        other => plan_err!("generator outer guard cannot build a one-element NULL {other}"),
    }
}

fn cardinality_of(arg: &Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::collection::cardinality_udf(),
        vec![arg.clone()],
    ))
}

fn normalize_list(expr: Expr, arg_type: &DataType) -> Result<Expr> {
    match arg_type {
        DataType::List(_) => Ok(expr),
        other => {
            let Some(field) = list_element(other) else {
                return plan_err!("generator requires a list argument, got {other}");
            };
            Ok(Expr::Cast(Cast::new(
                Box::new(expr),
                DataType::List(Arc::clone(field)),
            )))
        }
    }
}

fn guarded(outer: bool, cond: &Expr, list_expr: Expr, schema: &DFSchema) -> Result<Expr> {
    if !outer {
        return Ok(list_expr);
    }
    let list_type = match list_expr.get_type(schema)? {
        list @ DataType::List(_) => list,
        other => {
            let Some(field) = list_element(&other) else {
                return plan_err!("generator outer guard requires a list argument, got {other}");
            };
            DataType::List(Arc::clone(field))
        }
    };
    let then_arm = one_element_null_list(&list_type)?;
    let else_expr = if matches!(list_expr.get_type(schema)?, DataType::List(_)) {
        list_expr
    } else {
        Expr::Cast(Cast::new(Box::new(list_expr), list_type))
    };
    when(cond.clone(), then_arm).otherwise(else_expr)
}

fn ordinality_call(list_expr: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(GeneratorOrdinality::new())),
        vec![list_expr],
    ))
}

fn collection_output(
    name: &str,
    arg_type: &DataType,
    outer: bool,
) -> Result<(bool, Vec<OutputSpec>)> {
    if let Some(item) = list_element(arg_type) {
        return Ok((
            false,
            vec![OutputSpec {
                member: GENERATOR_COL_FIELD.to_string(),
                name: "col".to_string(),
                nullable: outer || item.is_nullable(),
            }],
        ));
    }
    let DataType::Map(entries, _) = arg_type else {
        return Err(unexpected_input(name, arg_type, "\"ARRAY\" or \"MAP\""));
    };
    let DataType::Struct(pair) = entries.data_type() else {
        return Err(unexpected_input(name, arg_type, "\"ARRAY\" or \"MAP\""));
    };
    if pair.len() != 2 {
        return Err(unexpected_input(name, arg_type, "\"ARRAY\" or \"MAP\""));
    }
    let key_field = &pair[0];
    let value_field = &pair[1];
    Ok((
        true,
        vec![
            OutputSpec {
                member: GENERATOR_KEY_FIELD.to_string(),
                name: "key".to_string(),
                nullable: outer || key_field.is_nullable(),
            },
            OutputSpec {
                member: GENERATOR_VALUE_FIELD.to_string(),
                name: "value".to_string(),
                nullable: outer || value_field.is_nullable(),
            },
        ],
    ))
}

fn inline_output(name: &str, arg_type: &DataType, outer: bool) -> Result<Vec<OutputSpec>> {
    let Some(item) = list_element(arg_type) else {
        return Err(unexpected_input(name, arg_type, "\"ARRAY\""));
    };
    let DataType::Struct(fields) = item.data_type() else {
        return Err(unexpected_input(
            name,
            arg_type,
            "\"ARRAY\" whose element type is composed of \"STRUCT\"",
        ));
    };
    Ok(fields
        .iter()
        .map(|field| OutputSpec {
            member: field.name().clone(),
            name: field.name().clone(),
            nullable: outer || item.is_nullable() || field.is_nullable(),
        })
        .collect())
}

fn rewrite_projection(
    projection: &Projection,
    site_index: usize,
    kind: GeneratorKind,
    args: &[Expr],
    alias_names: Option<&[String]>,
) -> Result<LogicalPlan> {
    let Some(arg) = args.first() else {
        return plan_err!("generator rewrite lost the generator argument");
    };
    let input_schema = projection.input.schema().clone();
    let arg_type = arg.get_type(input_schema.as_ref())?;
    let outer = kind.outer();
    let cond = arg.clone().is_null().or(cardinality_of(arg).eq(lit(0_i32)));
    let (lists, mut outputs, flatten_struct) = match kind {
        GeneratorKind::JsonTuple => {
            return json_tuple::rewrite_json_tuple(projection, site_index, args, alias_names);
        }
        GeneratorKind::Inline { .. } => {
            let list_arg = normalize_list(arg.clone(), &arg_type)?;
            (
                vec![(
                    GENERATOR_FIELD,
                    guarded(outer, &cond, list_arg, &input_schema)?,
                )],
                inline_output(kind.label(), &arg_type, outer)?,
                true,
            )
        }
        GeneratorKind::PosExplode { .. } => {
            let (is_map, mut specs) = collection_output(kind.label(), &arg_type, outer)?;
            let mut lists: Vec<(&'static str, Expr)> = Vec::with_capacity(3);
            if is_map {
                lists.push((
                    GENERATOR_POS_FIELD,
                    ordinality_call(nested_fn::map_keys(arg.clone())),
                ));
                lists.push((
                    GENERATOR_KEY_FIELD,
                    guarded(
                        outer,
                        &cond,
                        nested_fn::map_keys(arg.clone()),
                        &input_schema,
                    )?,
                ));
                lists.push((
                    GENERATOR_VALUE_FIELD,
                    guarded(
                        outer,
                        &cond,
                        nested_fn::map_values(arg.clone()),
                        &input_schema,
                    )?,
                ));
            } else {
                let list_arg = normalize_list(arg.clone(), &arg_type)?;
                lists.push((GENERATOR_POS_FIELD, ordinality_call(list_arg.clone())));
                lists.push((
                    GENERATOR_COL_FIELD,
                    guarded(outer, &cond, list_arg, &input_schema)?,
                ));
            }
            specs.insert(
                0,
                OutputSpec {
                    member: GENERATOR_POS_FIELD.to_string(),
                    name: "pos".to_string(),
                    nullable: outer,
                },
            );
            (lists, specs, false)
        }
    };
    if let Some(names) = alias_names {
        if names.len() != outputs.len() {
            return plan_err!(
                "[COLUMN_ALIASES_MISMATCH] The number of aliases supplied in the AS clause \
                 does not match the number of columns output by the generator: expected {}, \
                 got {}",
                outputs.len(),
                names.len()
            );
        }
        for (spec, name) in outputs.iter_mut().zip(names.iter()) {
            spec.name.clone_from(name);
        }
    }
    build_expansion(
        projection,
        site_index,
        &input_schema,
        &lists,
        &outputs,
        outer,
        flatten_struct,
    )
}

fn build_expansion(
    projection: &Projection,
    site_index: usize,
    input_schema: &Arc<DFSchema>,
    lists: &[(&'static str, Expr)],
    outputs: &[OutputSpec],
    outer: bool,
    flatten_struct: bool,
) -> Result<LogicalPlan> {
    let mut inner_exprs: Vec<Expr> = Vec::with_capacity(projection.expr.len() - 1 + lists.len());
    for (index, expr) in projection.expr.iter().enumerate() {
        if index != site_index {
            inner_exprs.push(expr.clone());
        }
    }
    for (name, expr) in lists {
        inner_exprs.push(expr.clone().alias(*name));
    }
    let inner = LogicalPlanBuilder::from(projection.input.as_ref().clone())
        .project(inner_exprs)?
        .build()?;
    let unnest_columns: Vec<Column> = lists
        .iter()
        .map(|(name, _)| Column::from_name(*name))
        .collect();
    let unnested = LogicalPlanBuilder::from(inner)
        .unnest_columns_with_options(
            unnest_columns,
            UnnestOptions::new().with_preserve_nulls(outer),
        )?
        .build()?;
    let unnested_schema = unnested.schema().clone();
    let passthrough_count = projection.expr.len() - 1;
    let mut passthrough_fields = unnested_schema.iter().take(passthrough_count);
    let struct_ref = Expr::Column(Column::new_unqualified(GENERATOR_FIELD));
    let mut final_exprs: Vec<Expr> = Vec::with_capacity(passthrough_count + outputs.len());
    for index in 0..projection.expr.len() {
        if index == site_index {
            for spec in outputs {
                let base = if flatten_struct {
                    Expr::ScalarFunction(ScalarFunction::new_udf(
                        Arc::new(ScalarUDF::from(GeneratorField::new())),
                        vec![struct_ref.clone(), lit(spec.member.clone())],
                    ))
                } else {
                    Expr::Column(Column::new_unqualified(spec.member.clone()))
                };
                let mut expr = base;
                if !spec.nullable {
                    expr = Expr::ScalarFunction(ScalarFunction::new_udf(
                        crate::decimal_cast::spark_nonnull_udf(),
                        vec![expr],
                    ));
                }
                final_exprs.push(expr.alias(spec.name.clone()));
            }
            continue;
        }
        let Some((qualifier, field)) = passthrough_fields.next() else {
            return plan_err!("generator rewrite lost a passthrough field");
        };
        final_exprs.push(Expr::Column(Column::new(
            qualifier.cloned(),
            field.name().clone(),
        )));
    }
    let mut final_fields: Vec<(Option<TableReference>, FieldRef)> =
        Vec::with_capacity(final_exprs.len());
    for expr in &final_exprs {
        final_fields.push(expr.to_field(unnested_schema.as_ref())?);
    }
    let schema = DFSchema::new_with_metadata(final_fields, input_schema.metadata().clone())?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        final_exprs,
        Arc::new(unnested),
        Arc::new(schema),
    )?))
}

#[cfg(test)]
mod tests;
