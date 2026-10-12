use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ExprSchema, Result};
use datafusion::logical_expr::expr::Cast;
use datafusion::logical_expr::{
    DmlStatement, Expr, ExprSchemable, LogicalPlan, Projection, Values, WriteOp,
};
use datafusion::sql::sqlparser::ast::{
    DataType as SqlDataType, Expr as SqlExpr, SetExpr, Statement,
};
use repark_functions::timestamp_ns_cast::{
    is_temporal_source, timestamp_ns_cast_expr, timestamp_ns_target,
};
use repark_iceberg::write::update_cast::{NtzDecision, nested_micro_ntz_decision};

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
    let nested = iceberg_insert(&plan) && dml_target_has_nested_micro_ntz(&plan);
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

fn iceberg_insert(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Dml(dml) = plan else {
        return false;
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return false;
    }
    let Ok(provider) = datafusion::datasource::source_as_provider(&dml.target) else {
        return false;
    };
    (provider.as_ref() as &dyn std::any::Any)
        .downcast_ref::<iceberg_datafusion::IcebergTableProvider>()
        .is_some()
}

fn dml_target_has_nested_micro_ntz(plan: &LogicalPlan) -> bool {
    let LogicalPlan::Dml(dml) = plan else {
        return false;
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return false;
    }
    dml.target.schema().fields().iter().any(|field| {
        !matches!(
            nested_micro_ntz_decision(None, field.data_type()),
            NtzDecision::Ineligible
        )
    })
}

fn wrap_nested_ntz(expr: &Expr, target: &DataType, schema: &DFSchema) -> Expr {
    let (body, alias) = split_alias(expr);
    let (value, cast_field) = match body {
        Expr::Cast(cast) if cast.field.data_type() == target => {
            (cast.expr.as_ref(), Some(Arc::clone(&cast.field)))
        }
        other => (other, None),
    };
    let Ok(source) = value.get_type(schema) else {
        return expr.clone();
    };
    let NtzDecision::Rebuild(paths) = nested_micro_ntz_decision(Some(&source), target) else {
        return expr.clone();
    };
    if repark_functions::nested_ntz_cast::NESTED_NTZ_CAST_NAME
        != repark_iceberg::write::ntz_store::NESTED_NTZ_WALL_CAST_UDF_NAME
    {
        return expr.clone();
    }
    let call =
        repark_functions::nested_ntz_cast::nested_ntz_cast_expr(value.clone(), &paths_text(&paths));
    let casted = match cast_field {
        Some(field) => Expr::Cast(Cast::new_from_field(Box::new(call), field)),
        None => Expr::Cast(Cast::new(Box::new(call), target.clone())),
    };
    match alias {
        Some(name) => casted.alias(name),
        None => casted,
    }
}

fn paths_text(paths: &[Vec<String>]) -> String {
    paths
        .iter()
        .map(|path| path.join("."))
        .collect::<Vec<_>>()
        .join(",")
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

    use super::{after_analysis, paths_text, wrap_nested_ntz};

    fn path_from_expr(expr: &Expr) -> Option<String> {
        use datafusion::common::ScalarValue;
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        let mut found = None;
        let _ = expr.apply(|node| {
            if let Expr::ScalarFunction(func) = node {
                if func.name() == "__repark_cast_nested_ntz__" {
                    if let Some(Expr::Literal(value, _)) = func.args.get(1) {
                        found = match value {
                            ScalarValue::Utf8(text)
                            | ScalarValue::LargeUtf8(text)
                            | ScalarValue::Utf8View(text) => text.clone(),
                            _ => None,
                        };
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
        });
        found
    }

    fn path_from_sql(sql: &str) -> Option<String> {
        let marker = "__repark_cast_nested_ntz__((";
        let start = sql.find(marker)? + marker.len();
        let after = &sql[start..];
        let lit = after.find(", '")? + 3;
        let mut body = String::new();
        let mut chars = after[lit..].chars().peekable();
        while let Some(ch) = chars.next() {
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    chars.next();
                    body.push('\'');
                    continue;
                }
                return Some(body);
            }
            body.push(ch);
        }
        None
    }

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
        let wrapped = [
            (
                struct_type(&[("v", instant()), ("n", DataType::Int32)]),
                struct_type(&[("v", ntz()), ("n", DataType::Int32)]),
                "v",
            ),
            (
                struct_type(&[("a", instant()), ("b", instant())]),
                struct_type(&[("a", ntz()), ("b", ntz())]),
                "a,b",
            ),
            (
                struct_type(&[(
                    "i",
                    struct_type(&[("v", instant()), ("n", DataType::Int32)]),
                )]),
                struct_type(&[("i", struct_type(&[("v", ntz()), ("n", DataType::Int32)]))]),
                "i.v",
            ),
        ];
        for (source, target, expected) in wrapped {
            let schema = Arc::new(
                DFSchema::try_from(Schema::new(vec![Field::new("st", source.clone(), true)]))
                    .expect("schema"),
            );
            let expr = col("st")
                .cast_to(&target, schema.as_ref())
                .expect("cast")
                .alias("st");
            let shown = wrap_nested_ntz(&expr, &target, schema.as_ref());
            let sql = repark_iceberg::write::update_cast::nested_micro_ntz_wall_sql(
                "`st`",
                Some(&source),
                &target,
            )
            .expect("sql");
            assert_eq!(path_from_expr(&shown).as_deref(), Some(expected), "{shown}");
            assert_eq!(path_from_sql(&sql).as_deref(), Some(expected), "{sql}");
        }
        let kept = [
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
        for (source, target) in kept {
            let schema = Arc::new(
                DFSchema::try_from(Schema::new(vec![Field::new("st", source.clone(), true)]))
                    .expect("schema"),
            );
            let expr = col("st").alias("st");
            let shown = wrap_nested_ntz(&expr, &target, schema.as_ref());
            assert_eq!(path_from_expr(&shown), None, "{shown}");
            assert!(
                repark_iceberg::write::update_cast::nested_micro_ntz_wall_sql(
                    "`st`",
                    Some(&source),
                    &target,
                )
                .is_none(),
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
        assert!(text.contains("__repark_cast_nested_ntz__"), "{text}");
        assert!(!text.contains("random"), "{text}");
        assert!(!text.contains("rand("), "{text}");
        assert!(!text.contains("get_field"), "{text}");
        assert_ne!(wrapped, expr);
    }

    #[test]
    fn a_non_column_source_is_projected_once() {
        let mut args = Vec::new();
        let mut fields = Vec::new();
        for index in 0..19 {
            let name = format!("f{index:02}");
            args.push(datafusion::logical_expr::lit(name.clone()));
            args.push(datafusion::logical_expr::lit(1_i32));
            fields.push(Field::new(&name, DataType::Int32, true));
        }
        args.push(datafusion::logical_expr::lit("v"));
        args.push(col("l"));
        fields.push(Field::new("v", ntz(), true));
        let target = DataType::Struct(fields.into());
        let input_fields = vec![
            Field::new("id", DataType::Int32, true),
            Field::new("l", instant(), true),
        ];
        let schema =
            Arc::new(DFSchema::try_from(Schema::new(input_fields.clone())).expect("input schema"));
        let value = datafusion::functions::expr_fn::named_struct(args);
        let st = value
            .cast_to(&target, schema.as_ref())
            .expect("cast")
            .alias("st");
        let shown = wrap_nested_ntz(&st, &target, schema.as_ref()).to_string();
        assert!(!shown.contains("random"), "{shown}");
        assert!(!shown.contains("rand("), "{shown}");
        assert!(!shown.contains("get_field"), "{shown}");
        assert_eq!(
            shown.matches("__repark_cast_nested_ntz__").count(),
            1,
            "{shown}"
        );
        assert_eq!(shown.matches("named_struct").count(), 1, "{shown}");
        let plan = plan_of(
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("st", target, true),
            ],
            input_fields,
            vec![col("id").alias("id"), st],
        );
        assert_eq!(after_analysis(plan.clone()).expect("plan"), plan);
        let LogicalPlan::Dml(dml) = plan else {
            panic!("dml");
        };
        let update = LogicalPlan::Dml(datafusion::logical_expr::dml::DmlStatement {
            op: WriteOp::Update,
            ..dml
        });
        assert_eq!(after_analysis(update.clone()).expect("update"), update);

        let column_target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let column_source = struct_type(&[("v", instant()), ("n", DataType::Int32)]);
        let column_fields = vec![Field::new("st", column_source, true)];
        let column_schema = Arc::new(
            DFSchema::try_from(Schema::new(column_fields.clone())).expect("column schema"),
        );
        let column_expr = col("st")
            .cast_to(&column_target, column_schema.as_ref())
            .expect("cast")
            .alias("st");
        let column_plan = plan_of(
            vec![Field::new("st", column_target.clone(), true)],
            column_fields,
            vec![column_expr.clone()],
        );
        assert_eq!(
            after_analysis(column_plan.clone()).expect("column plan"),
            column_plan
        );
        let column_text =
            wrap_nested_ntz(&column_expr, &column_target, column_schema.as_ref()).to_string();
        assert!(
            column_text.contains("__repark_cast_nested_ntz__"),
            "{column_text}"
        );
        assert!(!column_text.contains("random"), "{column_text}");
        assert!(!column_text.contains("rand("), "{column_text}");
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
        assert_eq!(text.matches("IS NULL").count(), 0, "{text}");
        assert!(text.contains("__repark_cast_nested_ntz__"), "{text}");
        assert!(
            text.contains("Utf8(\"v\")") || text.contains("'v'"),
            "{text}"
        );

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
        assert_eq!(text2.matches("IS NULL").count(), 0, "{text2}");
        assert!(text2.contains("i.v"), "{text2}");
    }

    #[test]
    fn a_user_cast_to_another_struct_stays_the_expression() {
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let source = struct_type(&[("v", instant()), ("n", DataType::Int32)]);
        let other = struct_type(&[("v", DataType::Date32), ("n", DataType::Int32)]);
        let schema = Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new("s", source, true)])).expect("schema"),
        );
        let cast = col("s")
            .cast_to(&other, schema.as_ref())
            .expect("cast")
            .alias("st");
        assert_eq!(wrap_nested_ntz(&cast, &target, schema.as_ref()), cast);
        let tried = Expr::TryCast(datafusion::logical_expr::expr::TryCast {
            expr: Box::new(col("s")),
            field: Arc::new(Field::new("st", other, true)),
        });
        assert_eq!(wrap_nested_ntz(&tried, &target, schema.as_ref()), tried);
    }

    #[test]
    fn the_path_literal_matches_the_sql_emitter() {
        let source = struct_type(&[("v", instant()), ("n", DataType::Int32)]);
        let target = struct_type(&[("v", ntz()), ("n", DataType::Int32)]);
        let paths = repark_iceberg::write::update_cast::nested_micro_ntz_selected_paths(
            Some(&source),
            &target,
        );
        let spark = format!("'{}'", paths_text(&paths).replace('\'', "''"));
        let iceberg = repark_iceberg::write::update_cast::nested_ntz_paths_literal(&paths);
        assert_eq!(spark, iceberg);
        assert_eq!(iceberg, "'v'");
    }
}
