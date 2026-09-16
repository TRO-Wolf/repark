use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{Column, DFSchema, Result, ScalarValue, TableReference, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, ScalarUDF,
};
use datafusion::prelude::lit;

use super::{GENERATOR_FIELD, GeneratorField, OutputSpec};

fn json_tuple_display(expr: &Expr) -> String {
    match expr {
        Expr::Column(column) => column.name.clone(),
        Expr::Literal(
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
            _,
        ) => text.clone(),
        Expr::Literal(ScalarValue::Null, _) => "NULL".to_string(),
        _ => expr.schema_name().to_string(),
    }
}

pub(crate) fn rewrite_json_tuple(
    projection: &Projection,
    site_index: usize,
    args: &[Expr],
    alias_names: Option<&[String]>,
) -> Result<LogicalPlan> {
    let input_schema = projection.input.schema().clone();
    let shape = args
        .iter()
        .map(json_tuple_display)
        .collect::<Vec<_>>()
        .join(", ");
    for arg in args {
        let data_type = arg.get_type(input_schema.as_ref())?;
        if !matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ) {
            return plan_err!(
                "[DATATYPE_MISMATCH.NON_STRING_TYPE] Cannot resolve \"json_tuple({shape})\" due \
                 to data type mismatch: all arguments of the function `json_tuple` must be \
                 strings. SQLSTATE: 42K09"
            );
        }
    }
    let mut outputs: Vec<OutputSpec> = (0..args.len().saturating_sub(1))
        .map(|index| OutputSpec {
            member: format!("c{index}"),
            name: format!("c{index}"),
            nullable: true,
        })
        .collect();
    if let Some(names) = alias_names {
        let lone_alias_on_single_output = names.len() == 1 && outputs.len() == 1;
        if !lone_alias_on_single_output {
            if names.len() != outputs.len() {
                return plan_err!(
                    "[UDTF_ALIAS_NUMBER_MISMATCH] The number of aliases supplied in the AS clause \
                     does not match the number of columns output by the UDTF."
                );
            }
            for (spec, name) in outputs.iter_mut().zip(names.iter()) {
                spec.name.clone_from(name);
            }
        }
    }
    let call = Expr::ScalarFunction(ScalarFunction::new_udf(
        crate::json::json_tuple_udf(),
        args.to_vec(),
    ));
    build_json_tuple_expansion(projection, site_index, &input_schema, call, &outputs)
}

fn build_json_tuple_expansion(
    projection: &Projection,
    site_index: usize,
    input_schema: &Arc<DFSchema>,
    call: Expr,
    outputs: &[OutputSpec],
) -> Result<LogicalPlan> {
    let mut inner_exprs: Vec<Expr> = Vec::with_capacity(projection.expr.len());
    for (index, expr) in projection.expr.iter().enumerate() {
        if index != site_index {
            inner_exprs.push(expr.clone());
        }
    }
    inner_exprs.push(call.alias(GENERATOR_FIELD));
    let inner = LogicalPlanBuilder::from(projection.input.as_ref().clone())
        .project(inner_exprs)?
        .build()?;
    let inner_schema = inner.schema().clone();
    let passthrough_count = projection.expr.len() - 1;
    let mut passthrough_fields = inner_schema.iter().take(passthrough_count);
    let struct_ref = Expr::Column(Column::new_unqualified(GENERATOR_FIELD));
    let mut final_exprs: Vec<Expr> = Vec::with_capacity(passthrough_count + outputs.len());
    for index in 0..projection.expr.len() {
        if index == site_index {
            for spec in outputs {
                let base = Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::new(ScalarUDF::from(GeneratorField::new())),
                    vec![struct_ref.clone(), lit(spec.member.clone())],
                ));
                final_exprs.push(base.alias(spec.name.clone()));
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
        final_fields.push(expr.to_field(inner_schema.as_ref())?);
    }
    let schema = DFSchema::new_with_metadata(final_fields, input_schema.metadata().clone())?;
    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        final_exprs,
        Arc::new(inner),
        Arc::new(schema),
    )?))
}
