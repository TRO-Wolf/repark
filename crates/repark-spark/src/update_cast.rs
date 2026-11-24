use datafusion::arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    Assignment, AssignmentTarget, CastKind, DataType as SqlDataType, Expr, Function, FunctionArg,
    FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, ObjectName, Statement,
    TableFactor, UnaryOperator, Update, Value,
};
use iceberg::{NamespaceIdent, TableIdent};
use repark_core::CatalogRegistry;
use repark_iceberg::catalog::uuid_presentation::presented_arrow_schema;
use repark_iceberg::write::negated_null_store::refuse_negated_null_writes;
use repark_iceberg::write::ntz_store::refuse_ntz_writes;
use repark_iceberg::write::struct_pairing::StructPairing;
use repark_iceberg::write::update_cast::{
    incompatible_paired_store_message, incompatible_paired_update_message,
};
use repark_iceberg::write::void_store::refuse_void_writes;

use crate::merge::nested_assign::{self, AssignmentScope};
use crate::void_type::{is_string_type, source_type_is_reliable};

struct UpdateTarget {
    arrow_schema: ArrowSchema,
}

async fn load_update_target(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    update: &Update,
) -> Option<UpdateTarget> {
    let object_name = crate::object_name_from_table_with_joins(&update.table)?;
    let parts = crate::name_parts(object_name);
    if parts.is_empty() {
        return None;
    }
    let qualified = crate::write_to_branch::qualify_table_parts(ctx, parts);
    if qualified.len() < 3 {
        return None;
    }
    let catalog = catalogs.get(&qualified[0])?;
    let namespace = NamespaceIdent::from_vec(qualified[1..qualified.len() - 1].to_vec()).ok()?;
    let ident = TableIdent::new(namespace, qualified[qualified.len() - 1].clone());
    let table = catalog.load_table(&ident).await.ok()?;
    let arrow_schema = presented_arrow_schema(table.metadata().current_schema()).ok()?;
    Some(UpdateTarget { arrow_schema })
}

pub(crate) async fn refuse_cast_then_fold_nested(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    update: &Update,
) -> Result<Option<String>> {
    let folded_assignments = if let Some(object_name) =
        crate::object_name_from_table_with_joins(&update.table)
        && let Some(target) = load_update_target(ctx, catalogs, update).await
    {
        let alias = match &update.table.relation {
            TableFactor::Table {
                alias: Some(alias), ..
            } => Some(alias.name.value.clone()),
            _ => None,
        };
        let written = crate::name_parts(object_name);
        let qualifiers = alias.clone().map_or_else(
            || nested_assign::name_suffixes(&written),
            |alias| vec![vec![alias]],
        );
        let scope = AssignmentScope {
            value_qualifiers: qualifiers.clone(),
            qualifiers,
            sql_qualifier: alias.unwrap_or_else(|| written.join(".")),
            column_prefix: None,
            probe_from: update.table.to_string(),
            case_sensitive: !crate::spark_door_case_insensitive(ctx.state().config().options()),
        };
        if !nested_assign::repeats_a_column(&target.arrow_schema, &scope, &update.assignments) {
            refuse_incompatible_update_cast(ctx, &target, object_name, update).await?;
        }
        nested_assign::fold_nested_assignments(
            ctx,
            &target.arrow_schema,
            &scope,
            &update.assignments,
        )
        .await?
    } else {
        None
    };
    let mut folded = update.clone();
    let mut changed = false;
    if let Some(assignments) = folded_assignments {
        folded.assignments = assignments;
        changed = true;
    }
    if has_spark_cast_seam(ctx) && wrap_explicit_update_casts(&mut folded.assignments) {
        changed = true;
    }
    if !changed {
        return Ok(None);
    }
    let mut rendered = Statement::Update(folded);
    Ok(Some(repark_iceberg::write::sql_text::render_for_reparse(
        &mut rendered,
    )))
}

fn has_spark_cast_seam(ctx: &SessionContext) -> bool {
    let state = ctx.state();
    if !state
        .scalar_functions()
        .contains_key(repark_functions::cast_map::ARROW_CAST_OPERAND_NAME)
    {
        return false;
    }
    let rules = &state.analyzer().rules;
    let has_rewrite = rules
        .iter()
        .any(|rule| rule.name() == "spark_string_integral_cast");
    let has_mark = rules
        .iter()
        .any(|rule| rule.name() == "spark_coercion_cast_mark");
    has_rewrite && has_mark
}

fn is_spark_cast_target(data_type: &SqlDataType) -> bool {
    match data_type {
        SqlDataType::TinyInt(_)
        | SqlDataType::SmallInt(_)
        | SqlDataType::Int2(_)
        | SqlDataType::Int16
        | SqlDataType::Int(_)
        | SqlDataType::Int4(_)
        | SqlDataType::Integer(_)
        | SqlDataType::Int32
        | SqlDataType::BigInt(_)
        | SqlDataType::Int8(_)
        | SqlDataType::Int64
        | SqlDataType::Signed
        | SqlDataType::SignedInteger
        | SqlDataType::Decimal(_)
        | SqlDataType::Dec(_)
        | SqlDataType::Numeric(_)
        | SqlDataType::BigDecimal(_)
        | SqlDataType::BigNumeric(_) => true,
        SqlDataType::Custom(name, _) => matches!(
            name.to_string().to_ascii_lowercase().as_str(),
            "byte"
                | "short"
                | "long"
                | "tinyint"
                | "smallint"
                | "int"
                | "integer"
                | "bigint"
                | "decimal"
                | "dec"
                | "numeric"
        ),
        _ => false,
    }
}

fn arrow_cast_operand_call(inner: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName::from(vec![Ident::new(
            repark_functions::cast_map::ARROW_CAST_OPERAND_NAME,
        )]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn wrap_explicit_update_casts(assignments: &mut [Assignment]) -> bool {
    let mut changed = false;
    for assignment in assignments {
        let mut peeled = &assignment.value;
        while let Expr::Nested(inner) = peeled {
            peeled = inner;
        }
        if matches!(
            peeled,
            Expr::Cast {
                kind: CastKind::Cast | CastKind::DoubleColon,
                data_type,
                ..
            } if is_spark_cast_target(data_type)
        ) {
            let inner = std::mem::replace(
                &mut assignment.value,
                Expr::Value(Value::Null.with_empty_span()),
            );
            assignment.value = arrow_cast_operand_call(inner);
            changed = true;
        }
    }
    changed
}

fn integer_literal_source_type(value: &Expr) -> Option<ArrowDataType> {
    let mut peeled = value;
    while let Expr::Nested(inner)
    | Expr::UnaryOp {
        op: UnaryOperator::Plus | UnaryOperator::Minus,
        expr: inner,
    } = peeled
    {
        peeled = inner;
    }
    let Expr::Value(literal) = peeled else {
        return None;
    };
    let Value::Number(text, long) = &literal.value else {
        return None;
    };
    match crate::void_type::number_text_type(text, *long) {
        typed @ (ArrowDataType::Int32 | ArrowDataType::Int64) => Some(typed),
        _ => None,
    }
}

async fn refuse_incompatible_update_cast(
    ctx: &SessionContext,
    target: &UpdateTarget,
    object_name: &ObjectName,
    update: &Update,
) -> Result<()> {
    let table_sql = object_name.to_string();
    let pairing = StructPairing::by_name(!crate::spark_door_case_insensitive(
        ctx.state().config().options(),
    ));
    for assignment in &update.assignments {
        let AssignmentTarget::ColumnName(name) = &assignment.target else {
            continue;
        };
        let target_parts: Vec<String> = name
            .0
            .iter()
            .filter_map(|part| part.as_ident().map(|ident| ident.value.clone()))
            .collect();
        if target_parts.len() != 1 {
            continue;
        }
        let column = &target_parts[0];
        let mut resolved = None;
        for field in target.arrow_schema.fields() {
            if field.name() == column {
                resolved = Some(field);
                break;
            }
            if resolved.is_none() && field.name().eq_ignore_ascii_case(column) {
                resolved = Some(field);
            }
        }
        let Some(field) = resolved else {
            continue;
        };
        let probe_sql = format!(
            "SELECT ({}) FROM {table_sql}",
            crate::void_type::parenthesize_stacked_minus(&assignment.value)
        );
        let Ok(frame) = ctx.sql(&probe_sql).await else {
            return Ok(());
        };
        let pair = [(column.as_str(), field.data_type())];
        refuse_void_writes(ctx, "``", frame.logical_plan(), pair)?;
        let pair = [(column.as_str(), field.data_type())];
        refuse_negated_null_writes(ctx, "``", frame.logical_plan(), pair)?;
        let pair = [(column.as_str(), field.data_type())];
        refuse_ntz_writes(ctx, "``", frame.logical_plan(), pair)?;
        if let Some(literal) = integer_literal_source_type(&assignment.value)
            && let Some(text) = repark_iceberg::write::update_cast::incompatible_update_message(
                "``",
                &format!("`{column}`"),
                &literal,
                field.data_type(),
            )
        {
            return Err(DataFusionError::Plan(text));
        }
        let Some(source_field) = frame.schema().fields().first() else {
            return Ok(());
        };
        let source = source_field.data_type();
        let column = format!("`{column}`");
        let judged =
            incompatible_paired_update_message(pairing, "``", &column, source, field.data_type())
                .is_some()
                || (!is_string_type(source)
                    && source_type_is_reliable(&assignment.value, field.data_type()));
        if judged
            && let Some(text) =
                incompatible_paired_store_message(pairing, "``", &column, source, field.data_type())
        {
            return Err(DataFusionError::Plan(text));
        }
    }
    Ok(())
}
