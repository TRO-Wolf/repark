use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::{ScalarValue, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    DmlStatement, Expr as DataFusionExpr, LogicalPlan, Projection as DfProjection, WriteOp,
};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Query, SelectItem, SetExpr, Statement, TableObject,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{Literal, PrimitiveLiteral, Schema as IcebergSchema};
use iceberg::table::Table;
use iceberg::{Catalog, ErrorKind, NamespaceIdent, TableIdent};

pub struct ColumnDefault {
    data_type: ArrowDataType,
    primitive: PrimitiveLiteral,
}

impl ColumnDefault {
    pub(crate) fn scalar(&self) -> Result<ScalarValue> {
        primitive_scalar(&self.data_type, &self.primitive)
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn sql_text(&self) -> Result<String> {
        default_sql_text(&self.data_type, &self.primitive)
    }
}

pub struct ColumnDefaults {
    fills: HashMap<String, ColumnDefault>,
    required: HashSet<String>,
}

impl ColumnDefaults {
    pub(crate) fn new() -> Self {
        Self {
            fills: HashMap::new(),
            required: HashSet::new(),
        }
    }

    #[must_use]
    pub fn get(&self, column: &str) -> Option<&ColumnDefault> {
        self.fills.get(column)
    }

    pub(crate) fn is_required(&self, column: &str) -> bool {
        self.required.contains(column)
    }

    pub(crate) fn has_any(&self) -> bool {
        !self.fills.is_empty()
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn column_defaults(schema: &IcebergSchema) -> Result<ColumnDefaults> {
    let arrow_schema =
        schema_to_arrow_schema(schema).map_err(crate::catalog::iceberg_to_datafusion)?;
    let mut defaults = ColumnDefaults::new();
    for (field, arrow_field) in schema
        .as_struct()
        .fields()
        .iter()
        .zip(arrow_schema.fields())
    {
        let key = field.name.to_ascii_lowercase();
        if field.required {
            defaults.required.insert(key.clone());
        }
        if let Some(Literal::Primitive(primitive)) = &field.write_default {
            defaults.fills.insert(
                key,
                ColumnDefault {
                    data_type: arrow_field.data_type().clone(),
                    primitive: primitive.clone(),
                },
            );
        }
    }
    Ok(defaults)
}

pub struct OverwriteSource {
    pub columns: Vec<String>,
    pub sql: String,
}

#[allow(clippy::missing_errors_doc)]
pub fn overwrite_source_with_defaults(
    schema: &IcebergSchema,
    listed: &[String],
    reserved: &[String],
    source: &Query,
) -> Result<OverwriteSource> {
    let plain = format!("SELECT * FROM ({source}) AS _repark_ow_src");
    if listed.is_empty() || !schema_has_write_default(schema) {
        return Ok(OverwriteSource {
            columns: listed.to_vec(),
            sql: plain,
        });
    }
    let defaults = column_defaults(schema)?;
    let mut columns = listed.to_vec();
    let mut fills = Vec::new();
    for field in schema.as_struct().fields() {
        let provided = |name: &String| name.eq_ignore_ascii_case(&field.name);
        if columns.iter().any(provided) || reserved.iter().any(provided) {
            continue;
        }
        if let Some(fill) = defaults.get(&field.name.to_ascii_lowercase()) {
            fills.push(format!(
                "({}) AS {}",
                fill.sql_text()?,
                crate::write::idents::quote_ident_spark(&field.name)
            ));
            columns.push(field.name.clone());
        }
    }
    if fills.is_empty() {
        return Ok(OverwriteSource {
            columns,
            sql: plain,
        });
    }
    Ok(OverwriteSource {
        columns,
        sql: format!(
            "SELECT *, {} FROM ({source}) AS _repark_ow_src",
            fills.join(", ")
        ),
    })
}

#[must_use]
pub fn schema_has_write_default(schema: &IcebergSchema) -> bool {
    schema
        .as_struct()
        .fields()
        .iter()
        .any(|field| matches!(field.write_default, Some(Literal::Primitive(_))))
}

pub fn insert_column_list(statement: &Statement) -> Option<Vec<String>> {
    let Statement::Insert(insert) = statement else {
        return None;
    };
    if insert.columns.is_empty() {
        return None;
    }
    Some(insert.columns.iter().map(column_object_name).collect())
}

#[must_use]
pub fn insert_target(statement: &Statement) -> Option<(String, TableIdent)> {
    let Statement::Insert(insert) = statement else {
        return None;
    };
    let TableObject::TableName(name) = &insert.table else {
        return None;
    };
    object_name_target(name)
}

#[must_use]
pub fn dml_target(plan: &LogicalPlan) -> Option<(String, TableIdent)> {
    let LogicalPlan::Dml(dml) = plan else {
        return None;
    };
    table_reference_target(&dml.table_name)
}

pub struct MarkerRewrite {
    pub rewritten: Option<String>,
    pub preloaded: Option<Table>,
}

impl MarkerRewrite {
    #[must_use]
    pub fn unchanged() -> Self {
        Self {
            rewritten: None,
            preloaded: None,
        }
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn rewrite_insert_markers(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    statement: &mut Statement,
) -> Result<MarkerRewrite> {
    let Statement::Insert(insert) = statement else {
        return Ok(MarkerRewrite::unchanged());
    };
    let Some(source) = insert.source.as_mut() else {
        return Ok(MarkerRewrite::unchanged());
    };
    if !query_has_default_marker(source) {
        return Ok(MarkerRewrite::unchanged());
    }
    let table = match catalog.load_table(ident).await {
        Ok(table) => table,
        Err(error) if error.kind() == ErrorKind::TableNotFound => {
            return Ok(MarkerRewrite::unchanged());
        }
        Err(error) => return Err(crate::catalog::iceberg_to_datafusion(error)),
    };
    let current = table.metadata().current_schema();
    let arrow_schema =
        schema_to_arrow_schema(current).map_err(crate::catalog::iceberg_to_datafusion)?;
    let fields: Vec<(String, Option<ColumnDefault>)> = current
        .as_struct()
        .fields()
        .iter()
        .zip(arrow_schema.fields())
        .map(|(field, arrow_field)| {
            let fill = match &field.write_default {
                Some(Literal::Primitive(primitive)) => Some(ColumnDefault {
                    data_type: arrow_field.data_type().clone(),
                    primitive: primitive.clone(),
                }),
                _ => None,
            };
            (field.name.clone(), fill)
        })
        .collect();
    let positions: Vec<MarkerFill> = if insert.columns.is_empty() {
        fields
            .iter()
            .map(|(_, fill)| fill_fill(fill.as_ref()))
            .collect::<Result<Vec<MarkerFill>>>()?
    } else {
        insert
            .columns
            .iter()
            .map(|name| resolve_listed_field(&fields, &column_object_name(name)))
            .collect::<Result<Vec<MarkerFill>>>()?
    };
    let mut changed = false;
    match source.body.as_mut() {
        SetExpr::Values(values) => {
            for row in &mut values.rows {
                for (index, cell) in row.content.iter_mut().enumerate() {
                    if substitute_marker(cell, positions.get(index))? {
                        changed = true;
                    }
                }
            }
        }
        SetExpr::Select(select) => {
            for (index, item) in select.projection.iter_mut().enumerate() {
                let cell: &mut SqlExpr = match item {
                    SelectItem::UnnamedExpr(cell) => cell,
                    SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => continue,
                };
                if substitute_marker(cell, positions.get(index))? {
                    changed = true;
                }
            }
        }
        _ => {}
    }
    if changed {
        return Ok(MarkerRewrite {
            rewritten: Some(statement.to_string()),
            preloaded: Some(table),
        });
    }
    Ok(MarkerRewrite {
        rewritten: None,
        preloaded: Some(table),
    })
}

fn query_has_default_marker(source: &Query) -> bool {
    match source.body.as_ref() {
        SetExpr::Values(values) => values
            .rows
            .iter()
            .flat_map(|row| row.content.iter())
            .any(is_default_marker),
        SetExpr::Select(select) => select.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(cell) => is_default_marker(cell),
            SelectItem::ExprWithAlias { expr, .. } => is_default_marker(expr),
            _ => false,
        }),
        _ => false,
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn fill_insert_plan(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    listed: Option<&[String]>,
    plan: LogicalPlan,
    preloaded: Option<Table>,
) -> Result<LogicalPlan> {
    let Some(listed) = listed else {
        return Ok(plan);
    };
    let LogicalPlan::Dml(dml) = plan else {
        return Ok(plan);
    };
    if !matches!(dml.op, WriteOp::Insert(_)) {
        return Ok(LogicalPlan::Dml(dml));
    }
    let table = match preloaded {
        Some(table) => table,
        None => catalog
            .load_table(ident)
            .await
            .map_err(crate::catalog::iceberg_to_datafusion)?,
    };
    let defaults = column_defaults(table.metadata().current_schema())?;
    if !defaults.has_any() {
        return Ok(LogicalPlan::Dml(dml));
    }
    let input = Arc::clone(&dml.input);
    let LogicalPlan::Projection(projection) = input.as_ref() else {
        return Ok(LogicalPlan::Dml(dml));
    };
    let listed: HashSet<String> = listed
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect();
    let mut exprs: Vec<DataFusionExpr> = Vec::with_capacity(projection.expr.len());
    let mut changed = false;
    for expr in &projection.expr {
        exprs.push(fill_pad_expr(&defaults, &listed, expr)?);
        if exprs.last() != Some(expr) {
            changed = true;
        }
    }
    if !changed {
        return Ok(LogicalPlan::Dml(dml));
    }
    let filled =
        LogicalPlan::Projection(DfProjection::try_new(exprs, Arc::clone(&projection.input))?);
    Ok(LogicalPlan::Dml(DmlStatement {
        input: Arc::new(filled),
        ..dml
    }))
}

enum MarkerFill {
    Fill(String),
    Null,
    Leave,
}

fn column_object_name(name: &datafusion::sql::sqlparser::ast::ObjectName) -> String {
    let parts: Vec<String> = name
        .0
        .iter()
        .filter_map(|part| part.as_ident())
        .map(|ident| ident.value.clone())
        .collect();
    if parts.len() == name.0.len() && parts.len() == 1 {
        return parts[0].clone();
    }
    name.to_string()
}

fn object_name_parts(name: &datafusion::sql::sqlparser::ast::ObjectName) -> Option<Vec<String>> {
    let parts: Vec<String> = name
        .0
        .iter()
        .map(|part| part.as_ident().map(|ident| ident.value.clone()))
        .collect::<Option<Vec<String>>>()?;
    if parts.iter().any(String::is_empty) {
        return None;
    }
    Some(parts)
}

fn object_name_target(
    name: &datafusion::sql::sqlparser::ast::ObjectName,
) -> Option<(String, TableIdent)> {
    let parts = object_name_parts(name)?;
    if parts.len() != 3 {
        return None;
    }
    let namespace = NamespaceIdent::from_vec(vec![parts[1].clone()]).ok()?;
    Some((
        parts[0].clone(),
        TableIdent::new(namespace, parts[2].clone()),
    ))
}

fn table_reference_target(reference: &TableReference) -> Option<(String, TableIdent)> {
    let namespace = NamespaceIdent::from_vec(vec![reference.schema()?.to_string()]).ok()?;
    Some((
        reference.catalog()?.to_string(),
        TableIdent::new(namespace, reference.table().to_string()),
    ))
}

fn resolve_listed_field(
    fields: &[(String, Option<ColumnDefault>)],
    listed: &str,
) -> Result<MarkerFill> {
    if let Some((_, fill)) = fields.iter().find(|(name, _)| name == listed) {
        return fill_fill(fill.as_ref());
    }
    if let Some((_, fill)) = fields
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(listed))
    {
        return fill_fill(fill.as_ref());
    }
    Ok(MarkerFill::Leave)
}

fn fill_fill(fill: Option<&ColumnDefault>) -> Result<MarkerFill> {
    match fill {
        Some(fill) => Ok(MarkerFill::Fill(fill.sql_text()?)),
        None => Ok(MarkerFill::Null),
    }
}

fn is_default_marker(expr: &SqlExpr) -> bool {
    match expr {
        SqlExpr::Identifier(ident) => {
            ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case("default")
        }
        _ => false,
    }
}

fn substitute_marker(cell: &mut SqlExpr, fill: Option<&MarkerFill>) -> Result<bool> {
    if !is_default_marker(cell) {
        return Ok(false);
    }
    let text = match fill {
        Some(MarkerFill::Fill(text)) => text.clone(),
        Some(MarkerFill::Null) => "NULL".to_string(),
        Some(MarkerFill::Leave) | None => return Ok(false),
    };
    *cell = parse_fragment(&text)?;
    Ok(true)
}

fn parse_fragment(text: &str) -> Result<SqlExpr> {
    let statements =
        Parser::parse_sql(&GenericDialect {}, &format!("SELECT {text}")).map_err(|error| {
            DataFusionError::Plan(format!("cannot parse write default `{text}`: {error}"))
        })?;
    let Some(Statement::Query(query)) = statements.into_iter().next() else {
        return Err(DataFusionError::Plan(format!(
            "cannot parse write default `{text}`: expected a SELECT"
        )));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(DataFusionError::Plan(format!(
            "cannot parse write default `{text}`: expected a SELECT"
        )));
    };
    let Some(SelectItem::UnnamedExpr(expr)) = select.projection.first() else {
        return Err(DataFusionError::Plan(format!(
            "cannot parse write default `{text}`: expected one SELECT item"
        )));
    };
    Ok(expr.clone())
}

fn fill_pad_expr(
    defaults: &ColumnDefaults,
    listed: &HashSet<String>,
    expr: &DataFusionExpr,
) -> Result<DataFusionExpr> {
    let DataFusionExpr::Alias(alias) = expr else {
        return Ok(expr.clone());
    };
    if listed.contains(&alias.name.to_ascii_lowercase()) {
        return Ok(expr.clone());
    }
    if !null_stripped(&alias.expr) {
        return Ok(expr.clone());
    }
    let key = alias.name.to_ascii_lowercase();
    match defaults.get(&key) {
        Some(fill) => Ok(DataFusionExpr::Literal(fill.scalar()?, None).alias(&alias.name)),
        None if defaults.is_required(&key) => Err(DataFusionError::Plan(format!(
            "INSERT leaves required column `{}` unassigned with no write default",
            alias.name
        ))),
        None => Ok(expr.clone()),
    }
}

fn null_stripped(expr: &DataFusionExpr) -> bool {
    match expr {
        DataFusionExpr::Literal(scalar, _) => scalar.is_null(),
        DataFusionExpr::Cast(cast) => null_stripped(&cast.expr),
        DataFusionExpr::TryCast(try_cast) => null_stripped(&try_cast.expr),
        _ => false,
    }
}

fn primitive_scalar(
    data_type: &ArrowDataType,
    primitive: &PrimitiveLiteral,
) -> Result<ScalarValue> {
    match (data_type, primitive) {
        (ArrowDataType::Int32, PrimitiveLiteral::Int(value)) => {
            Ok(ScalarValue::Int32(Some(*value)))
        }
        (ArrowDataType::Int64, PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::Int64(Some(*value)))
        }
        (ArrowDataType::Int64, PrimitiveLiteral::Int(value)) => {
            Ok(ScalarValue::Int64(Some(i64::from(*value))))
        }
        (ArrowDataType::Float32, PrimitiveLiteral::Float(value)) => {
            Ok(ScalarValue::Float32(Some(value.0)))
        }
        (ArrowDataType::Float64, PrimitiveLiteral::Double(value)) => {
            Ok(ScalarValue::Float64(Some(value.0)))
        }
        (ArrowDataType::Float64, PrimitiveLiteral::Float(value)) => {
            Ok(ScalarValue::Float64(Some(f64::from(value.0))))
        }
        (ArrowDataType::Utf8, PrimitiveLiteral::String(value)) => {
            Ok(ScalarValue::Utf8(Some(value.clone())))
        }
        (ArrowDataType::LargeUtf8, PrimitiveLiteral::String(value)) => {
            Ok(ScalarValue::LargeUtf8(Some(value.clone())))
        }
        (ArrowDataType::Boolean, PrimitiveLiteral::Boolean(value)) => {
            Ok(ScalarValue::Boolean(Some(*value)))
        }
        (ArrowDataType::Date32, PrimitiveLiteral::Int(value)) => {
            Ok(ScalarValue::Date32(Some(*value)))
        }
        (ArrowDataType::Time64(TimeUnit::Microsecond), PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::Time64Microsecond(Some(*value)))
        }
        (ArrowDataType::Timestamp(TimeUnit::Microsecond, None), PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::TimestampMicrosecond(Some(*value), None))
        }
        (
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(zone)),
            PrimitiveLiteral::Long(value),
        ) => Ok(ScalarValue::TimestampMicrosecond(
            Some(*value),
            Some(Arc::clone(zone)),
        )),
        (ArrowDataType::Timestamp(TimeUnit::Nanosecond, None), PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::TimestampNanosecond(Some(*value), None))
        }
        (
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, Some(zone)),
            PrimitiveLiteral::Long(value),
        ) => Ok(ScalarValue::TimestampNanosecond(
            Some(*value),
            Some(Arc::clone(zone)),
        )),
        (ArrowDataType::Decimal128(precision, scale), PrimitiveLiteral::Int128(value)) => {
            Ok(ScalarValue::Decimal128(Some(*value), *precision, *scale))
        }
        (ArrowDataType::Binary, PrimitiveLiteral::Binary(value)) => {
            Ok(ScalarValue::Binary(Some(value.clone())))
        }
        (ArrowDataType::LargeBinary, PrimitiveLiteral::Binary(value)) => {
            Ok(ScalarValue::LargeBinary(Some(value.clone())))
        }
        (ArrowDataType::FixedSizeBinary(16), PrimitiveLiteral::UInt128(value)) => Ok(
            ScalarValue::FixedSizeBinary(16, Some(value.to_be_bytes().to_vec())),
        ),
        (ArrowDataType::FixedSizeBinary(width), PrimitiveLiteral::Binary(value))
            if usize::try_from(*width).is_ok_and(|bound| value.len() == bound) =>
        {
            Ok(ScalarValue::FixedSizeBinary(*width, Some(value.clone())))
        }
        _ => Err(DataFusionError::Plan(format!(
            "write default `{primitive:?}` does not fit column type `{data_type}`"
        ))),
    }
}

fn default_sql_text(data_type: &ArrowDataType, primitive: &PrimitiveLiteral) -> Result<String> {
    let (literal, sql_type) = match (data_type, primitive) {
        (ArrowDataType::Int32, PrimitiveLiteral::Int(value)) => {
            (value.to_string(), "INT".to_string())
        }
        (ArrowDataType::Int64, PrimitiveLiteral::Long(value)) => {
            (value.to_string(), "BIGINT".to_string())
        }
        (ArrowDataType::Int64, PrimitiveLiteral::Int(value)) => {
            (value.to_string(), "BIGINT".to_string())
        }
        (ArrowDataType::Float32, PrimitiveLiteral::Float(value)) => {
            (float_text(f64::from(value.0))?, "FLOAT".to_string())
        }
        (ArrowDataType::Float64, PrimitiveLiteral::Double(value)) => {
            (float_text(value.0)?, "DOUBLE".to_string())
        }
        (ArrowDataType::Float64, PrimitiveLiteral::Float(value)) => {
            (float_text(f64::from(value.0))?, "DOUBLE".to_string())
        }
        (ArrowDataType::Utf8 | ArrowDataType::LargeUtf8, PrimitiveLiteral::String(value)) => {
            (string_text(value), "VARCHAR".to_string())
        }
        (ArrowDataType::Boolean, PrimitiveLiteral::Boolean(true)) => {
            ("TRUE".to_string(), "BOOLEAN".to_string())
        }
        (ArrowDataType::Boolean, PrimitiveLiteral::Boolean(false)) => {
            ("FALSE".to_string(), "BOOLEAN".to_string())
        }
        (ArrowDataType::Date32, PrimitiveLiteral::Int(value)) => {
            let (year, month, day) = civil_from_days(i64::from(*value))?;
            (
                format!("'{year:04}-{month:02}-{day:02}'"),
                "DATE".to_string(),
            )
        }
        (ArrowDataType::Time64(TimeUnit::Microsecond), PrimitiveLiteral::Long(value)) => {
            (format!("'{}'", clock_text(*value)), "TIME".to_string())
        }
        (ArrowDataType::Timestamp(TimeUnit::Microsecond, None), PrimitiveLiteral::Long(value)) => (
            format!("'{}'", stamp_text(*value)?),
            "TIMESTAMP".to_string(),
        ),
        (
            ArrowDataType::Timestamp(TimeUnit::Microsecond, Some(_)),
            PrimitiveLiteral::Long(value),
        ) => (
            format!("'{}+00:00'", stamp_text(*value)?),
            "TIMESTAMP WITH TIME ZONE".to_string(),
        ),
        (ArrowDataType::Decimal128(precision, scale), PrimitiveLiteral::Int128(value)) => (
            format!("'{}'", decimal_text(*value, *scale)?),
            format!("DECIMAL({precision},{scale})"),
        ),
        _ => {
            return Err(DataFusionError::Plan(format!(
                "write default `{primitive:?}` has no SQL rendering for column type `{data_type}`"
            )));
        }
    };
    Ok(format!("CAST({literal} AS {sql_type})"))
}

fn float_text(value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(DataFusionError::Plan(format!(
            "write default `{value}` is not finite"
        )));
    }
    Ok(value.to_string())
}

fn string_text(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn decimal_text(value: i128, scale: i8) -> Result<String> {
    if scale < 0 {
        return Err(DataFusionError::Plan(format!(
            "write default scale `{scale}` is negative"
        )));
    }
    let digits = value.abs().to_string();
    let text = if scale == 0 {
        digits
    } else {
        let width = usize::from(u8::try_from(scale).map_err(|_| {
            DataFusionError::Plan(format!("write default scale `{scale}` overflows"))
        })?);
        let padded = format!("{digits:0>width$}", width = width + 1);
        let (head, tail) = padded.split_at(padded.len() - width);
        format!("{head}.{tail}")
    };
    if value < 0 {
        return Ok(format!("-{text}"));
    }
    Ok(text)
}

fn civil_from_days(days: i64) -> Result<(i32, u32, u32)> {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let ordinal = shifted.rem_euclid(146_097);
    let year_of_era = (ordinal - ordinal / 1_460 + ordinal / 36_524 - ordinal / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = ordinal - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_pair = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_pair + 2) / 5 + 1;
    let month = if month_pair < 10 {
        month_pair + 3
    } else {
        month_pair - 9
    };
    let year = if month <= 2 { year + 1 } else { year };
    let narrow = |value: i64, what: &str| {
        u32::try_from(value)
            .map_err(|_| DataFusionError::Plan(format!("civil {what} `{value}` overflows")))
    };
    Ok((
        i32::try_from(year)
            .map_err(|_| DataFusionError::Plan(format!("civil year `{year}` overflows")))?,
        narrow(month, "month")?,
        narrow(day, "day")?,
    ))
}

fn clock_text(micros: i64) -> String {
    let day = 86_400_000_000i64;
    let rest = micros.rem_euclid(day);
    let hour = rest / 3_600_000_000;
    let minute = rest % 3_600_000_000 / 60_000_000;
    let second = rest % 60_000_000 / 1_000_000;
    let fraction = rest % 1_000_000;
    format!("{hour:02}:{minute:02}:{second:02}.{fraction:06}")
}

fn stamp_text(micros: i64) -> Result<String> {
    let day = 86_400_000_000i64;
    let (year, month, day) = civil_from_days(micros.div_euclid(day))?;
    Ok(format!(
        "{year:04}-{month:02}-{day:02} {}",
        clock_text(micros)
    ))
}

#[cfg(test)]
mod tests;
