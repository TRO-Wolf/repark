use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ListArray, new_null_array};
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
mod udfs;

pub(crate) use udfs::GeneratorField;
use udfs::GeneratorOrdinality;
#[cfg(test)]
use udfs::ordinality;

pub(crate) const GENERATOR_ALIAS_UDF: &str = "__repark_gen_alias";

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

impl GeneratorPlaceholder {
    fn explodes_one_column(&self) -> bool {
        matches!(self.name, "explode" | "explode_outer")
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
        let first = arg_types.first().cloned().unwrap_or(DataType::Null);
        if self.explodes_one_column()
            && let Some(item) = list_element(&first)
        {
            return Ok(item.data_type().clone());
        }
        Ok(first)
    }

    fn return_field_from_args(
        &self,
        args: datafusion::logical_expr::ReturnFieldArgs,
    ) -> Result<FieldRef> {
        let first = args.arg_fields.first();
        if self.explodes_one_column()
            && let Some(item) = first.and_then(|field| list_element(field.data_type()))
        {
            let nullable = self.name == "explode_outer" || item.is_nullable();
            return Ok(Arc::new(Field::new(
                "col",
                item.data_type().clone(),
                nullable,
            )));
        }
        let data_type = first.map_or(DataType::Null, |field| field.data_type().clone());
        Ok(Arc::new(Field::new(self.name, data_type, true)))
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        if self.explodes_one_column() {
            return Ok("col".to_string());
        }
        let formatted: Vec<String> = args
            .iter()
            .map(|arg| arg.schema_name().to_string())
            .collect();
        Ok(format!("{}({})", self.name, formatted.join(",")))
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

#[must_use]
pub fn generator_udf(name: &'static str) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(GeneratorPlaceholder::new(name)))
}

#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        generator_udf("explode"),
        generator_udf("explode_outer"),
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
    Explode { outer: bool },
    PosExplode { outer: bool },
    Inline { outer: bool },
    JsonTuple,
}

impl GeneratorKind {
    fn label(self) -> &'static str {
        match self {
            Self::Explode { outer: false } => "explode",
            Self::Explode { outer: true } => "explode_outer",
            Self::PosExplode { outer: false } => "posexplode",
            Self::PosExplode { outer: true } => "posexplode_outer",
            Self::Inline { outer: false } => "inline",
            Self::Inline { outer: true } => "inline_outer",
            Self::JsonTuple => "json_tuple",
        }
    }

    fn outer(self) -> bool {
        match self {
            Self::Explode { outer } | Self::PosExplode { outer } | Self::Inline { outer } => outer,
            Self::JsonTuple => false,
        }
    }
}

fn generator_kind(name: &str) -> Option<GeneratorKind> {
    match name {
        "explode" => Some(GeneratorKind::Explode { outer: false }),
        "explode_outer" => Some(GeneratorKind::Explode { outer: true }),
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

#[must_use]
pub fn is_bare_generator_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function)
            if is_generator_call(function) || function.func.name() == GENERATOR_ALIAS_UDF
    )
}

#[must_use]
pub fn is_bare_explode_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::ScalarFunction(function)
            if matches!(function.func.name(), "explode" | "explode_outer")
    )
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

fn unqualified_call_name(function: &ScalarFunction) -> String {
    let args: Vec<String> = function
        .args
        .iter()
        .map(|arg| match arg {
            Expr::Column(column) => column.name.clone(),
            other => other.schema_name().to_string(),
        })
        .collect();
    format!("{}({})", function.func.name(), args.join(","))
}

fn restored_first_arg_matches(arg: &Expr, body: &str) -> bool {
    match arg {
        Expr::ScalarFunction(function)
            if matches!(
                function.func.name(),
                crate::decimal_cast::SPARK_NONNULL_NAME
                    | crate::decimal_cast::DECIMAL_CAST_NULLABLE_NAME
            ) =>
        {
            function
                .args
                .first()
                .is_some_and(|inner| restored_first_arg_matches(inner, body))
        }
        Expr::Column(column) => {
            body == column.name
                || body.ends_with(&format!(".{}", column.name))
                || body.starts_with(&format!("{},", column.name))
                || body.contains(&format!(".{},", column.name))
        }
        Expr::ScalarFunction(function) => {
            let callee = function.func.name();
            body.starts_with(&format!("{callee}("))
                || (callee == "map" && body.starts_with("create_map("))
                || (callee == "make_array" && body.starts_with("array("))
        }
        Expr::Cast(_) | Expr::TryCast(_) => {
            body.starts_with("CAST(") || body.starts_with("TRY_CAST(")
        }
        Expr::Literal(_, _) => body.starts_with("Utf8(") || body.starts_with("NULL"),
        _ => false,
    }
}

fn is_restored_generator_alias(inner: &Expr, alias_name: &str) -> bool {
    let Expr::ScalarFunction(function) = inner else {
        return false;
    };
    let name = function.func.name();
    if matches!(name, "explode" | "explode_outer") {
        return false;
    }
    if name == GENERATOR_ALIAS_UDF {
        return alias_name.starts_with(&format!("{GENERATOR_ALIAS_UDF}("));
    }
    if alias_name == inner.schema_name().to_string()
        || alias_name == unqualified_call_name(function)
    {
        return true;
    }
    let Some(tail) = alias_name.strip_prefix(&format!("{name}(")) else {
        return false;
    };
    let Some(body) = tail.strip_suffix(')') else {
        return false;
    };
    function
        .args
        .first()
        .is_some_and(|first| restored_first_arg_matches(first, body))
}

fn peel_generator(expr: &Expr) -> Result<Option<GeneratorSite>> {
    match expr {
        Expr::Alias(alias) => {
            let Some((kind, arg, inner_names)) = peel_generator(alias.expr.as_ref())? else {
                return Ok(None);
            };
            if is_restored_generator_alias(alias.expr.as_ref(), &alias.name) {
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
    if args.iter().any(&references_aggr) {
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
            let nullable_field = Arc::new(field.as_ref().clone().with_nullable(true));
            let values = new_null_array(nullable_field.data_type(), 1);
            let offsets = OffsetBuffer::new(vec![0_i32, 1].into());
            let array = ListArray::try_new(nullable_field, offsets, values, None)?;
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
    let orig_type = list_expr.get_type(schema)?;
    let Some(field) = list_element(&orig_type) else {
        return plan_err!("generator outer guard requires a list argument, got {orig_type}");
    };
    let target_type = if field.is_nullable() {
        DataType::List(Arc::clone(field))
    } else {
        DataType::List(Arc::new(field.as_ref().clone().with_nullable(true)))
    };
    let then_arm = one_element_null_list(&target_type)?;
    let else_expr = if orig_type == target_type {
        list_expr
    } else {
        Expr::Cast(Cast::new(Box::new(list_expr), target_type))
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
        GeneratorKind::Explode { .. } => {
            let (is_map, specs) = collection_output(kind.label(), &arg_type, outer)?;
            let lists = collection_lists(arg, &arg_type, is_map, outer, &cond, &input_schema)?;
            (lists, specs, false)
        }
        GeneratorKind::PosExplode { .. } => {
            let (is_map, mut specs) = collection_output(kind.label(), &arg_type, outer)?;
            let mut lists = collection_lists(arg, &arg_type, is_map, outer, &cond, &input_schema)?;
            let positions = if is_map {
                ordinality_call(nested_fn::map_keys(arg.clone()))
            } else {
                ordinality_call(normalize_list(arg.clone(), &arg_type)?)
            };
            lists.insert(0, (GENERATOR_POS_FIELD, positions));
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

fn collection_lists(
    arg: &Expr,
    arg_type: &DataType,
    is_map: bool,
    outer: bool,
    cond: &Expr,
    input_schema: &DFSchema,
) -> Result<Vec<(&'static str, Expr)>> {
    if !is_map {
        let list_arg = normalize_list(arg.clone(), arg_type)?;
        return Ok(vec![(
            GENERATOR_COL_FIELD,
            guarded(outer, cond, list_arg, input_schema)?,
        )]);
    }
    Ok(vec![
        (
            GENERATOR_KEY_FIELD,
            guarded(outer, cond, nested_fn::map_keys(arg.clone()), input_schema)?,
        ),
        (
            GENERATOR_VALUE_FIELD,
            guarded(
                outer,
                cond,
                nested_fn::map_values(arg.clone()),
                input_schema,
            )?,
        ),
    ])
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
