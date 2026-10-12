use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ExprSchema, Result};
use datafusion::functions::expr_fn::{get_field, named_struct, random};
use datafusion::logical_expr::expr::{BinaryExpr, Case, Cast};
use datafusion::logical_expr::{
    DmlStatement, Expr, ExprSchemable, LogicalPlan, Operator, Projection, Values, WriteOp, lit,
};
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, SetExpr, Statement,
};
use repark_functions::timestamp_ns_cast::{
    is_temporal_source, timestamp_ns_cast_expr, timestamp_ns_target,
};
use repark_functions::timestamp_ntz_cast::{timestamp_ntz_cast_expr, timestamp_ntz_cast_udf};
use repark_iceberg::write::ntz_store::wall_cast_udf_name;
#[cfg(test)]
use repark_iceberg::write::update_cast::nested_micro_ntz_selected_paths;
use repark_iceberg::write::update_cast::{
    NtzDecision, NtzMember, NtzNode, nested_micro_ntz_decision,
};

pub(crate) fn before_analysis(
    plan: LogicalPlan,
    timestamp_cells: &[(usize, usize)],
) -> Result<LogicalPlan> {
    let Some(targets) = ns_store_targets(&plan) else {
        return Ok(plan);
    };
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(plan);
    };
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(LogicalPlan::Dml(dml));
    };
    let source_schema = Arc::clone(projection.input.schema());
    let mut exprs = Vec::with_capacity(projection.expr.len());
    let mut values_columns: Vec<(String, bool)> = Vec::new();
    let mut changed = false;
    for (expr, target) in projection.expr.iter().zip(&targets) {
        let Some(zoned) = *target else {
            exprs.push(expr.clone());
            continue;
        };
        let (inner, name) = split_alias(expr);
        if let Some(base) = peel_ns_casts(inner) {
            if is_string_column(base, &source_schema) {
                exprs.push(expr.clone());
                continue;
            }
            exprs.push(realias(timestamp_ns_cast_expr(base.clone(), zoned), name));
            changed = true;
            continue;
        }
        if let Expr::Column(column) = inner {
            values_columns.push((column.name.clone(), zoned));
        }
        exprs.push(expr.clone());
    }
    let input = match projection.input.as_ref() {
        LogicalPlan::Values(values) if !values_columns.is_empty() => {
            match conform_values_rows(values, &values_columns, timestamp_cells) {
                Some(rewritten) => {
                    changed = true;
                    Arc::new(LogicalPlan::Values(rewritten))
                }
                None => Arc::clone(&projection.input),
            }
        }
        _ => Arc::clone(&projection.input),
    };
    rebuild(dml, exprs, input, changed)
}

pub(crate) fn after_analysis(plan: LogicalPlan) -> Result<LogicalPlan> {
    let nested = dml_target_has_nested_micro_ntz(&plan);
    let targets = ns_store_targets(&plan);
    if targets.is_none() && !nested {
        return Ok(plan);
    }
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(plan);
    };
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return Ok(LogicalPlan::Dml(dml));
    };
    if projection.expr.len() != dml.target.schema().fields().len() {
        return Ok(LogicalPlan::Dml(dml));
    }
    let source_schema = Arc::clone(projection.input.schema());
    let mut exprs = Vec::with_capacity(projection.expr.len());
    let mut changed = false;
    let ns_targets = targets.unwrap_or_else(|| vec![None; projection.expr.len()]);
    for ((expr, target), field) in projection
        .expr
        .iter()
        .zip(&ns_targets)
        .zip(dml.target.schema().fields())
    {
        let Some(zoned) = *target else {
            let wrapped = wrap_nested_ntz(expr, field.data_type(), source_schema.as_ref());
            changed |= &wrapped != expr;
            exprs.push(wrapped);
            continue;
        };
        if matches!(dml.op, WriteOp::Update) {
            let table = dml.table_name.to_string();
            let narrowed = Narrowed {
                table: &table,
                column: field.name(),
                zoned,
            };
            narrowed.refuse(expr, source_schema.as_ref())?;
        }
        let Ok(found) = expr.get_type(source_schema.as_ref()) else {
            exprs.push(expr.clone());
            continue;
        };
        if found == *field.data_type() || !is_temporal_source(&found) {
            exprs.push(expr.clone());
            continue;
        }
        let (inner, name) = split_alias(expr);
        let name = name.unwrap_or_else(|| field.name().clone());
        exprs.push(realias(
            timestamp_ns_cast_expr(inner.clone(), zoned),
            Some(name),
        ));
        changed = true;
    }
    let input = Arc::clone(&projection.input);
    rebuild(dml, exprs, input, changed)
}

fn dml_target_has_nested_micro_ntz(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Dml(dml) = plan else {
        return false;
    };
    if !matches!(dml.op, WriteOp::Insert(_) | WriteOp::Update) {
        return false;
    }
    dml.target.schema().fields().iter().any(|field| {
        !matches!(
            nested_micro_ntz_decision(None, field.data_type()),
            NtzDecision::Ineligible
        )
    })
}

#[cfg(test)]
fn expr_selected_paths(source: Option<&DataType>, target: &DataType) -> Vec<Vec<String>> {
    nested_micro_ntz_selected_paths(source, target)
}

fn wrap_nested_ntz(expr: &Expr, target: &DataType, schema: &DFSchema) -> Expr {
    let (body, alias) = split_alias(expr);
    let (value, cast_field) = match body {
        Expr::Cast(cast) => (cast.expr.as_ref(), Some(Arc::clone(&cast.field))),
        other => (other, None),
    };
    let Ok(source) = value.get_type(schema) else {
        return expr.clone();
    };
    let NtzDecision::Rebuild(members) = nested_micro_ntz_decision(Some(&source), target) else {
        return expr.clone();
    };
    let built = emit_struct(value.clone(), &members, target);
    let casted = match cast_field {
        Some(field) => Expr::Cast(Cast::new_from_field(Box::new(built), field)),
        None => Expr::Cast(Cast::new(Box::new(built), target.clone())),
    };
    match alias {
        Some(name) => casted.alias(name),
        None => casted,
    }
}

fn emit_struct(expr: Expr, members: &[NtzMember], target: &DataType) -> Expr {
    let DataType::Struct(fields) = target else {
        return expr;
    };
    let mut args = Vec::with_capacity(members.len() * 2);
    for (member, field) in members.iter().zip(fields.iter()) {
        args.push(Expr::Literal(
            datafusion::common::ScalarValue::Utf8(Some(member.target_name.clone())),
            None,
        ));
        let child = get_field(kept(expr.clone()), member.source_name.as_str());
        let built = match &member.node {
            NtzNode::Leaf => ntz_kernel(child),
            NtzNode::Struct(inner) => Expr::Cast(Cast::new(
                Box::new(emit_struct(child, inner, field.data_type())),
                field.data_type().clone(),
            )),
            NtzNode::Keep => child,
        };
        args.push(built);
    }
    guarded_struct(expr, named_struct(args), target)
}

fn guarded_struct(source: Expr, built: Expr, target: &DataType) -> Expr {
    let null_struct = Expr::Cast(Cast::new(
        Box::new(lit(datafusion::common::ScalarValue::Null)),
        target.clone(),
    ));
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(
            Box::new(Expr::IsNull(Box::new(source))),
            Box::new(null_struct),
        )],
        else_expr: Some(Box::new(built)),
    })
}

fn kept(expr: Expr) -> Expr {
    let guard = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(random()),
        op: Operator::Lt,
        right: Box::new(lit(2_i32)),
    });
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(Box::new(guard), Box::new(expr.clone()))],
        else_expr: Some(Box::new(expr)),
    })
}

fn ntz_kernel(expr: Expr) -> Expr {
    let leaf = DataType::Timestamp(TimeUnit::Microsecond, None);
    match wall_cast_udf_name(&leaf) {
        Some(name) if name == timestamp_ntz_cast_udf(false).name() => {
            timestamp_ntz_cast_expr(expr, false)
        }
        _ => expr,
    }
}

struct Narrowed<'a> {
    table: &'a str,
    column: &'a str,
    zoned: bool,
}

impl Narrowed<'_> {
    fn refuse(&self, expr: &Expr, schema: &DFSchema) -> Result<()> {
        if !narrows(expr, schema) {
            return Ok(());
        }
        let Self {
            table,
            column,
            zoned,
        } = self;
        let (upper, lower) = if *zoned {
            ("TIMESTAMPTZ_NS", "timestamptz_ns")
        } else {
            ("TIMESTAMP_NS", "timestamp_ns")
        };
        Err(datafusion::error::DataFusionError::Plan(format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `{table}`: Cannot safely cast `{column}` \"TIMESTAMP\" to \"{upper}\". The \
             value was narrowed from nanoseconds to microseconds before the store; give the NULL \
             beside it the type {lower}. SQLSTATE: KD000"
        )))
    }
}

fn narrows(expr: &Expr, schema: &DFSchema) -> bool {
    let cut = |inner: &Expr, target: &DataType| {
        holds_timestamp(target, false)
            && !typed_null(inner)
            && inner
                .get_type(schema)
                .is_ok_and(|found| holds_timestamp(&found, true))
    };
    match expr {
        Expr::Cast(cast) if cut(&cast.expr, cast.field.data_type()) => true,
        Expr::TryCast(cast) if cut(&cast.expr, cast.field.data_type()) => true,
        Expr::Case(case) => {
            case.when_then_expr
                .iter()
                .any(|(_, then)| narrows(then, schema))
                || case
                    .else_expr
                    .as_deref()
                    .is_some_and(|other| narrows(other, schema))
        }
        other => {
            let mut found = false;
            let walked = other.apply_children(|child| {
                found = narrows(child, schema);
                Ok(if found {
                    TreeNodeRecursion::Stop
                } else {
                    TreeNodeRecursion::Continue
                })
            });
            walked.is_ok() && found
        }
    }
}

fn typed_null(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => scalar.is_null(),
        Expr::Cast(cast) => typed_null(&cast.expr),
        Expr::TryCast(cast) => typed_null(&cast.expr),
        _ => false,
    }
}

fn holds_timestamp(data_type: &DataType, nanoseconds: bool) -> bool {
    match data_type {
        DataType::Timestamp(unit, _) => (*unit == TimeUnit::Nanosecond) == nanoseconds,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| holds_timestamp(field.data_type(), nanoseconds)),
        DataType::Map(field, _)
        | DataType::List(field)
        | DataType::LargeList(field)
        | DataType::FixedSizeList(field, _) => holds_timestamp(field.data_type(), nanoseconds),
        _ => false,
    }
}

fn ns_store_targets(plan: &LogicalPlan) -> Option<Vec<Option<bool>>> {
    let LogicalPlan::Dml(dml) = plan else {
        return None;
    };
    if !matches!(dml.op, WriteOp::Insert(_) | WriteOp::Update) {
        return None;
    }
    let schema = dml.target.schema();
    let fields = schema.fields();
    let LogicalPlan::Projection(projection) = dml.input.as_ref() else {
        return None;
    };
    if projection.expr.len() != fields.len() {
        return None;
    }
    let targets: Vec<Option<bool>> = fields
        .iter()
        .map(|field| timestamp_ns_target(field.data_type()))
        .collect();
    targets.iter().any(Option::is_some).then_some(targets)
}

pub(crate) fn timestamp_typed_values_cells(statement: &Statement) -> Vec<(usize, usize)> {
    let Statement::Insert(insert) = statement else {
        return Vec::new();
    };
    let Some(source) = insert.source.as_deref() else {
        return Vec::new();
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Vec::new();
    };
    let mut cells = Vec::new();
    for (row, parens) in values.rows.iter().enumerate() {
        for (column, cell) in parens.content.iter().enumerate() {
            if is_timestamp_typed(cell) {
                cells.push((row, column));
            }
        }
    }
    cells
}

fn is_timestamp_typed(cell: &SqlExpr) -> bool {
    match cell {
        SqlExpr::Nested(inner) => is_timestamp_typed(inner),
        SqlExpr::Cast { data_type, .. } => matches!(data_type, SqlDataType::Timestamp(_, _)),
        SqlExpr::TypedString(typed) => matches!(typed.data_type, SqlDataType::Timestamp(_, _)),
        _ => false,
    }
}

fn rebuild(
    dml: DmlStatement,
    exprs: Vec<Expr>,
    input: Arc<LogicalPlan>,
    changed: bool,
) -> Result<LogicalPlan> {
    if !changed {
        return Ok(LogicalPlan::Dml(dml));
    }
    let conformed = LogicalPlan::Projection(Projection::try_new(exprs, input)?);
    Ok(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(conformed),
        ..dml
    }))
}

fn split_alias(expr: &Expr) -> (&Expr, Option<String>) {
    match expr {
        Expr::Alias(alias) => (alias.expr.as_ref(), Some(alias.name.clone())),
        other => (other, None),
    }
}

fn realias(expr: Expr, name: Option<String>) -> Expr {
    match name {
        Some(name) => expr.alias(name),
        None => expr,
    }
}

fn peel_ns_casts(expr: &Expr) -> Option<&Expr> {
    let Expr::Cast(cast) = expr else {
        return None;
    };
    timestamp_ns_target(cast.field.data_type())?;
    let mut base = cast.expr.as_ref();
    while let Expr::Cast(inner) = base
        && timestamp_ns_target(inner.field.data_type()).is_some()
    {
        base = inner.expr.as_ref();
    }
    Some(base)
}

fn is_string_column(expr: &Expr, schema: &DFSchema) -> bool {
    let Expr::Column(column) = expr else {
        return false;
    };
    schema.field_from_column(column).is_ok_and(|field| {
        matches!(
            field.data_type(),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        )
    })
}

fn conform_values_rows(
    values: &Values,
    columns: &[(String, bool)],
    timestamp_cells: &[(usize, usize)],
) -> Option<Values> {
    let mut rewrites: Vec<(usize, usize, Expr)> = Vec::new();
    for (name, zoned) in columns {
        let Some(index) = values.schema.index_of_column_by_name(None, name) else {
            continue;
        };
        for (row_index, row) in values.values.iter().enumerate() {
            if timestamp_cells.binary_search(&(row_index, index)).is_ok() {
                continue;
            }
            if let Some(base) = peel_ns_casts(&row[index]) {
                rewrites.push((
                    row_index,
                    index,
                    timestamp_ns_cast_expr(base.clone(), *zoned),
                ));
            }
        }
    }
    if rewrites.is_empty() {
        return None;
    }
    let mut rows = values.values.clone();
    for (row, column, expr) in rewrites {
        rows[row][column] = expr;
    }
    Some(Values {
        schema: Arc::clone(&values.schema),
        values: rows,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use datafusion::common::{DFSchema, TableReference};
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::logical_expr::{
        EmptyRelation, Expr, ExprSchemable, LogicalPlan, Projection, TableSource, WriteOp, col,
    };

    use super::{after_analysis, expr_selected_paths, wrap_nested_ntz};

    fn ntz() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }

    fn instant() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    }

    fn struct_type(pairs: &[(&str, DataType)]) -> DataType {
        DataType::Struct(
            pairs
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
                .collect(),
        )
    }

    struct Source(SchemaRef);

    impl TableSource for Source {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.0)
        }
    }

    fn plan_of(
        target_fields: Vec<Field>,
        input_fields: Vec<Field>,
        exprs: Vec<Expr>,
    ) -> LogicalPlan {
        let input_schema =
            Arc::new(DFSchema::try_from(Schema::new(input_fields)).expect("input schema"));
        let projection = Projection::try_new(
            exprs,
            Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema: input_schema,
            })),
        )
        .expect("projection");
        let target = Arc::new(Source(Arc::new(Schema::new(target_fields))));
        LogicalPlan::Dml(datafusion::logical_expr::dml::DmlStatement::new(
            TableReference::full("ice", "ns", "t"),
            target,
            WriteOp::Insert(InsertOp::Append),
            Arc::new(LogicalPlan::Projection(projection)),
        ))
    }

    #[test]
    fn both_emitters_select_the_same_paths() {
        let pairs = [
            (
                struct_type(&[("v", instant()), ("n", DataType::Int32)]),
                struct_type(&[("v", ntz()), ("n", DataType::Int32)]),
            ),
            (
                struct_type(&[("v", ntz()), ("n", DataType::Int32)]),
                struct_type(&[("v", ntz()), ("n", DataType::Int32)]),
            ),
            (
                struct_type(&[("v", DataType::Utf8)]),
                struct_type(&[("v", ntz())]),
            ),
            (
                struct_type(&[("v", DataType::Date32)]),
                struct_type(&[("v", ntz())]),
            ),
            (
                struct_type(&[("v", DataType::Int64)]),
                struct_type(&[("v", ntz())]),
            ),
            (
                struct_type(&[(
                    "i",
                    struct_type(&[("v", instant()), ("n", DataType::Int32)]),
                )]),
                struct_type(&[("i", struct_type(&[("v", ntz()), ("n", DataType::Int32)]))]),
            ),
            (instant(), ntz()),
            (
                DataType::List(Arc::new(Field::new(
                    "item",
                    struct_type(&[("v", instant())]),
                    true,
                ))),
                DataType::List(Arc::new(Field::new(
                    "item",
                    struct_type(&[("v", ntz())]),
                    true,
                ))),
            ),
        ];
        for (source, target) in pairs {
            assert_eq!(
                expr_selected_paths(Some(&source), &target),
                repark_iceberg::write::update_cast::nested_micro_ntz_selected_paths(
                    Some(&source),
                    &target
                ),
                "{source:?} -> {target:?}"
            );
        }
    }

    #[test]
    fn a_target_without_a_nested_micro_ntz_leaf_keeps_the_plan() {
        let cases = [
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("v", ntz(), true),
            ],
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("st", struct_type(&[("v", instant())]), true),
            ],
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new(
                    "st",
                    struct_type(&[("v", DataType::Timestamp(TimeUnit::Nanosecond, None))]),
                    true,
                ),
            ],
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("n", DataType::Int32, true),
            ],
        ];
        for fields in cases {
            let exprs = fields
                .iter()
                .map(|field| col(field.name()).alias(field.name()))
                .collect();
            let plan = plan_of(fields.clone(), fields, exprs);
            assert_eq!(after_analysis(plan.clone()).expect("plan"), plan);
        }
    }

    #[test]
    fn an_unselected_leaf_is_the_same_expression() {
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let source = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let schema = Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new("st", source, true)])).expect("schema"),
        );
        let expr = col("st").alias("st");
        assert_eq!(wrap_nested_ntz(&expr, &target, schema.as_ref()), expr);
    }

    #[test]
    fn an_instant_leaf_becomes_the_ntz_kernel() {
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let source = struct_type(&[("v", instant()), ("n", DataType::Int32)]);
        let schema = Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new("st", source, true)])).expect("schema"),
        );
        let expr = col("st")
            .cast_to(&target, schema.as_ref())
            .expect("cast")
            .alias("st");
        let wrapped = wrap_nested_ntz(&expr, &target, schema.as_ref());
        let text = wrapped.to_string();
        assert!(text.contains("__repark_cast_timestamp_ntz__"), "{text}");
        assert_ne!(wrapped, expr);
    }

    #[test]
    fn a_rebuilt_struct_guards_null_at_every_depth() {
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let source = struct_type(&[("v", instant()), ("n", DataType::Int32)]);
        let schema = Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new("st", source, true)])).expect("schema"),
        );
        let expr = col("st")
            .cast_to(&target, schema.as_ref())
            .expect("cast")
            .alias("st");
        let text = wrap_nested_ntz(&expr, &target, schema.as_ref()).to_string();
        assert_eq!(text.matches("IS NULL").count(), 1, "{text}");

        let inner = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let target2 = struct_type(&[("i", inner), ("n", DataType::Int32)]);
        let source2 = struct_type(&[
            (
                "i",
                struct_type(&[("v", instant()), ("n", DataType::Int32)]),
            ),
            ("n", DataType::Int32),
        ]);
        let schema2 = Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new("st", source2, true)])).expect("schema"),
        );
        let expr2 = col("st")
            .cast_to(&target2, schema2.as_ref())
            .expect("cast")
            .alias("st");
        let text2 = wrap_nested_ntz(&expr2, &target2, schema2.as_ref()).to_string();
        assert_eq!(text2.matches("IS NULL").count(), 2, "{text2}");
    }
}
