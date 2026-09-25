//! Partition-scoped INSERT OVERWRITE: static row-filter and dynamic replace-partitions.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, new_null_array,
};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{FieldRef, SchemaRef};
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Ident, UnaryOperator, Value, ValueWithSpan,
};
use iceberg::Catalog;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{DataFile, Datum, Literal, NestedField, PrimitiveType, Transform, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};

use crate::write::commit_error::{commit_result, operation_id_and_summary};
use crate::write::commit_target::{maybe_to_branch, snapshot_id_for_commit};
use crate::write::overwrite::{OverwriteIsolation, parse_overwrite_isolation};
use crate::write::overwrite_scope::replace_partitions_is_noop;
use crate::write::static_value::{cast_constant_array, cast_datum};
use crate::write::store_assign::refuse_unless_write_store_assignable;

/// A static equality (`k = v`) or a null (`k IS NULL` / `k = NULL`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionEquality {
    /// Partition field name or identity source column, as written.
    pub name: String,
    /// Literal to match, or `None` for NULL.
    pub value: Option<PartitionLiteral>,
}

/// A SQL-literal value that can bind to an identity partition column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionLiteral {
    /// Boolean.
    Boolean(bool),
    /// 32-bit integer.
    Int(i32),
    /// 64-bit integer.
    Long(i64),
    /// UTF-8 string.
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PartitionOverwriteRequest {
    pub equalities: Vec<PartitionEquality>,
    pub dynamic_names: Vec<String>,
    pub names: Vec<String>,
    pub refused_values: Vec<String>,
}

/// Static overwrite: row filter plus the equalities to inject into the source.
#[derive(Debug, Clone)]
pub struct StaticPartitionOverwrite {
    /// Iceberg row filter (`k = v AND …`).
    pub predicate: Predicate,
    /// Partition equalities, in PARTITION-clause order.
    pub equalities: Vec<PartitionEquality>,
}

#[allow(clippy::missing_errors_doc)]
pub fn partition_overwrite_request_from_exprs(
    expressions: &[Expr],
) -> Result<PartitionOverwriteRequest> {
    let mut request = PartitionOverwriteRequest::default();
    for expression in expressions {
        match partition_clause_item(expression)? {
            PartitionClauseItem::Dynamic(name) => {
                request.names.push(name.clone());
                request.dynamic_names.push(name);
            }
            PartitionClauseItem::Static(item) => {
                request.names.push(item.name.clone());
                request.equalities.push(item);
            }
            PartitionClauseItem::Refused(name, refusal) => {
                request.names.push(name);
                request.refused_values.push(refusal);
            }
        }
    }
    Ok(request)
}

fn store_assign_source_column(source: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    if source.data_type() == field.data_type() {
        return Ok(Arc::clone(source));
    }
    refuse_unless_write_store_assignable(
        "insert overwrite partition",
        field.name(),
        source.data_type(),
        field.data_type(),
    )?;
    Ok(cast_with_options(
        source,
        field.data_type(),
        &CastOptions {
            safe: false,
            ..CastOptions::default()
        },
    )?)
}

struct StaticPartitionPlan<'a> {
    table_schema: SchemaRef,
    by_source: HashMap<String, &'a PartitionEquality>,
    listed: Vec<String>,
}

impl<'a> StaticPartitionPlan<'a> {
    fn new(
        table_schema: SchemaRef,
        equalities: &'a [PartitionEquality],
        table: &Table,
    ) -> Result<Self> {
        let spec = table.metadata().default_partition_spec();
        let iceberg_schema = table.metadata().current_schema();
        let bindings = spec
            .fields()
            .iter()
            .map(|field| bind_partition_field(iceberg_schema.as_ref(), field))
            .collect::<Result<Vec<_>>>()?;
        let mut by_source: HashMap<String, &PartitionEquality> = HashMap::new();
        for equality in equalities {
            let binding = resolve_binding(&bindings, &equality.name)?;
            by_source.insert(binding.source_column_name.to_ascii_lowercase(), equality);
        }
        Ok(Self {
            table_schema,
            by_source,
            listed: Vec::new(),
        })
    }

    fn with_columns(mut self, columns: &[String]) -> Result<Self> {
        for column in columns {
            let key = column.to_ascii_lowercase();
            if self.by_source.contains_key(&key) {
                return Err(DataFusionError::Plan(format!(
                    "[STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST] Static partition column \
                     {column} is also specified in the column list. SQLSTATE: 42713"
                )));
            }
            if self.listed.contains(&key) {
                return Err(DataFusionError::Plan(format!(
                    "INSERT OVERWRITE column list names `{column}` more than once"
                )));
            }
            self.listed.push(key);
        }
        Ok(self)
    }

    fn inject_by_name(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if batch.num_columns() != self.listed.len() {
            return Err(DataFusionError::Plan(format!(
                "[INSERT_COLUMN_ARITY_MISMATCH] Cannot write to the target: the column list \
                 names {} columns, source has {}",
                self.listed.len(),
                batch.num_columns()
            )));
        }
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.table_schema.fields().len());
        for field in self.table_schema.fields() {
            let key = field.name().to_ascii_lowercase();
            if let Some(equality) = self.by_source.get(&key) {
                columns.push(constant_partition_array(
                    equality,
                    field.data_type(),
                    batch.num_rows(),
                )?);
            } else if let Some(index) = self.listed.iter().position(|name| *name == key) {
                columns.push(store_assign_source_column(batch.column(index), field)?);
            } else if field.is_nullable() {
                columns.push(new_null_array(field.data_type(), batch.num_rows()));
            } else {
                return Err(DataFusionError::Plan(format!(
                    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot find data for the \
                     output column `{}`",
                    field.name()
                )));
            }
        }
        RecordBatch::try_new(Arc::clone(&self.table_schema), columns).map_err(|error| {
            DataFusionError::Execution(format!(
                "INSERT OVERWRITE PARTITION failed to inject static partition columns: {error}"
            ))
        })
    }

    fn inject(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        if !self.listed.is_empty() {
            return self.inject_by_name(batch);
        }
        let expected_source = self
            .table_schema
            .fields()
            .len()
            .saturating_sub(self.by_source.len());
        if batch.num_columns() > expected_source {
            return Err(DataFusionError::Plan(format!(
                "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to the target, \
                 the reason is too many data columns: table has {} columns, static PARTITION \
                 injects {}, source has {}",
                self.table_schema.fields().len(),
                self.by_source.len(),
                batch.num_columns()
            )));
        }
        if batch.num_columns() != expected_source {
            return Err(DataFusionError::Plan(format!(
                "[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot write to the target: \
                 table has {} columns, static PARTITION injects {}, source has {}",
                self.table_schema.fields().len(),
                self.by_source.len(),
                batch.num_columns()
            )));
        }
        let mut source_index = 0usize;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.table_schema.fields().len());
        for field in self.table_schema.fields() {
            if let Some(equality) = self.by_source.get(&field.name().to_ascii_lowercase()) {
                columns.push(constant_partition_array(
                    equality,
                    field.data_type(),
                    batch.num_rows(),
                )?);
            } else {
                columns.push(store_assign_source_column(
                    batch.column(source_index),
                    field,
                )?);
                source_index += 1;
            }
        }
        RecordBatch::try_new(Arc::clone(&self.table_schema), columns).map_err(|error| {
            DataFusionError::Execution(format!(
                "INSERT OVERWRITE PARTITION failed to inject static partition columns: {error}"
            ))
        })
    }
}

/// Inject Hive static `PARTITION (k=v)` columns into a source batch (Spark arity).
/// # Errors
/// Too many source columns, missing source columns, or a literal that cannot fill `k`.
pub fn inject_static_partition_columns(
    batch: &RecordBatch,
    table_schema: &SchemaRef,
    equalities: &[PartitionEquality],
    table: &Table,
) -> Result<RecordBatch> {
    StaticPartitionPlan::new(Arc::clone(table_schema), equalities, table)?.inject(batch)
}

#[allow(clippy::missing_errors_doc)]
pub fn static_partition_source_columns(
    table: &Table,
    equalities: &[PartitionEquality],
) -> Result<Vec<String>> {
    let spec = table.metadata().default_partition_spec();
    let iceberg_schema = table.metadata().current_schema();
    let bindings = spec
        .fields()
        .iter()
        .map(|field| bind_partition_field(iceberg_schema.as_ref(), field))
        .collect::<Result<Vec<_>>>()?;
    equalities
        .iter()
        .map(|equality| {
            resolve_binding(&bindings, &equality.name)
                .map(|binding| binding.source_column_name.clone())
        })
        .collect()
}

/// Stage static-overwrite batches after injecting PARTITION (k=v) columns.
/// # Errors
/// Injection, positional map, or file write failures as [`DataFusionError`].
pub async fn stage_static_partition_overwrite_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    equalities: &[PartitionEquality],
    columns: &[String],
    concurrency: crate::write::concurrency::WriteConcurrency,
) -> Result<Vec<DataFile>> {
    let stream = static_injected_stream(table, batches, equalities, columns)?;
    crate::write::overwrite::write_overwrite_staged_files_from_stream(
        table,
        stream,
        Vec::new(),
        concurrency,
    )
    .await
}

pub(crate) fn static_injected_stream<'a>(
    table: &Table,
    batches: Vec<RecordBatch>,
    equalities: &'a [PartitionEquality],
    columns: &[String],
) -> Result<impl futures::Stream<Item = Result<RecordBatch>> + Unpin + use<'a>> {
    let write_schema: SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(iceberg_err)?,
    );
    let plan = StaticPartitionPlan::new(Arc::clone(&write_schema), equalities, table)?
        .with_columns(columns)?;
    Ok(futures::stream::iter(
        batches.into_iter().map(move |batch| plan.inject(&batch)),
    ))
}

fn constant_partition_array(
    equality: &PartitionEquality,
    data_type: &datafusion::arrow::datatypes::DataType,
    rows: usize,
) -> Result<ArrayRef> {
    use datafusion::arrow::datatypes::DataType;
    match (&equality.value, data_type) {
        (None, _) => Ok(new_null_array(data_type, rows)),
        (Some(PartitionLiteral::Boolean(flag)), DataType::Boolean) => {
            Ok(Arc::new(BooleanArray::from(vec![*flag; rows])))
        }
        (Some(PartitionLiteral::Int(value)), DataType::Int32) => {
            Ok(Arc::new(Int32Array::from(vec![*value; rows])))
        }
        (Some(PartitionLiteral::Int(value)), DataType::Int64) => {
            Ok(Arc::new(Int64Array::from(vec![i64::from(*value); rows])))
        }
        (Some(PartitionLiteral::Long(value)), DataType::Int64) => {
            Ok(Arc::new(Int64Array::from(vec![*value; rows])))
        }
        (
            Some(PartitionLiteral::String(text)),
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) => Ok(Arc::new(StringArray::from(vec![text.as_str(); rows]))),
        (Some(literal), other) => cast_constant_array(literal, other, rows),
    }
}

/// Commit a static partition overwrite: delete by row filter, add `staged_files`.
/// # Errors
/// Isolation parse, action apply, or catalog commit failures as [`DataFusionError`].
pub async fn commit_overwrite_by_row_filter(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    predicate: Predicate,
) -> Result<Table> {
    commit_overwrite_by_row_filter_to(catalog, table, staged_files, predicate, None).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_overwrite_by_row_filter_to(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    predicate: Predicate,
    branch: Option<&str>,
) -> Result<Table> {
    let isolation = parse_overwrite_isolation(table)?;
    let (operation_id, summary) = operation_id_and_summary();
    let tx = Transaction::new(table);
    let mut action = tx
        .overwrite_files()
        .overwrite_by_row_filter(predicate)
        .validate_added_files_match_overwrite_filter()
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    action = apply_overwrite_isolation(action, isolation, table, branch);
    let action = maybe_to_branch(table, action, branch, |action, name| action.to_branch(name))?;
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_replace_partitions(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
) -> Result<Table> {
    commit_replace_partitions_to(catalog, table, staged_files, None).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn commit_replace_partitions_to(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
    branch: Option<&str>,
) -> Result<Table> {
    if replace_partitions_is_noop(&staged_files) {
        return Ok(table.clone());
    }
    let isolation = parse_overwrite_isolation(table)?;
    let (operation_id, summary) = operation_id_and_summary();
    let tx = Transaction::new(table);
    let mut action = tx
        .replace_partitions()
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let action = maybe_to_branch(table, action, branch, |action, name| action.to_branch(name))?;
    let tx = action.apply(tx).map_err(iceberg_err)?;
    commit_result(tx.commit(catalog.as_ref()).await, &operation_id)
}

enum PartitionClauseItem {
    Dynamic(String),
    Static(PartitionEquality),
    Refused(String, String),
}

pub(crate) struct PartitionFieldBinding {
    pub(crate) spec_field_name: String,
    pub(crate) source_column_name: String,
    pub(crate) transform: Transform,
    primitive_type: PrimitiveType,
}

fn partition_clause_item(expression: &Expr) -> Result<PartitionClauseItem> {
    match expression {
        Expr::Nested(inner) => partition_clause_item(inner),
        Expr::Identifier(ident) => Ok(PartitionClauseItem::Dynamic(ident.value.clone())),
        Expr::CompoundIdentifier(parts) => Ok(PartitionClauseItem::Dynamic(last_ident(parts)?)),
        Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(text) | Value::SingleQuotedString(text),
            ..
        }) => Ok(PartitionClauseItem::Dynamic(text.clone())),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            let name = partition_name(left)?;
            match partition_literal(right) {
                Ok(value) => Ok(PartitionClauseItem::Static(PartitionEquality {
                    name,
                    value,
                })),
                Err(DataFusionError::Plan(refusal)) => {
                    Ok(PartitionClauseItem::Refused(name, refusal))
                }
                Err(other) => Err(other),
            }
        }
        other => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE PARTITION expected a name or k=v assignment, got `{other}`"
        ))),
    }
}

fn partition_name(expression: &Expr) -> Result<String> {
    match expression {
        Expr::Nested(inner) => partition_name(inner),
        Expr::Identifier(ident) => Ok(ident.value.clone()),
        Expr::CompoundIdentifier(parts) => last_ident(parts),
        Expr::Value(ValueWithSpan {
            value: Value::DoubleQuotedString(text) | Value::SingleQuotedString(text),
            ..
        }) => Ok(text.clone()),
        other => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE PARTITION assignment left side must be a column name, got `{other}`"
        ))),
    }
}

fn last_ident(parts: &[Ident]) -> Result<String> {
    parts
        .last()
        .map(|ident| ident.value.clone())
        .ok_or_else(|| {
            DataFusionError::Plan(
                "INSERT OVERWRITE PARTITION compound identifier is empty".to_string(),
            )
        })
}

fn partition_literal(expression: &Expr) -> Result<Option<PartitionLiteral>> {
    match expression {
        Expr::Nested(inner) => partition_literal(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match partition_literal(expr)? {
            Some(PartitionLiteral::Int(value)) => {
                let negated = value.checked_neg().ok_or_else(|| {
                    DataFusionError::Plan(
                        "INSERT OVERWRITE PARTITION integer negation overflows i32".to_string(),
                    )
                })?;
                Ok(Some(PartitionLiteral::Int(negated)))
            }
            Some(PartitionLiteral::Long(value)) => {
                let negated = value.checked_neg().ok_or_else(|| {
                    DataFusionError::Plan(
                        "INSERT OVERWRITE PARTITION integer negation overflows i64".to_string(),
                    )
                })?;
                Ok(Some(PartitionLiteral::Long(negated)))
            }
            other => Err(DataFusionError::Plan(format!(
                "INSERT OVERWRITE PARTITION cannot negate `{other:?}`"
            ))),
        },
        Expr::Value(ValueWithSpan {
            value: Value::Null, ..
        }) => Ok(None),
        Expr::Value(ValueWithSpan {
            value: Value::Boolean(flag),
            ..
        }) => Ok(Some(PartitionLiteral::Boolean(*flag))),
        Expr::Value(ValueWithSpan {
            value: Value::SingleQuotedString(text) | Value::DoubleQuotedString(text),
            ..
        }) => Ok(Some(PartitionLiteral::String(text.clone()))),
        Expr::Value(ValueWithSpan {
            value: Value::Number(raw, _),
            ..
        }) => parse_number_literal(raw),
        other => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE PARTITION assignment value must be a literal, got `{other}`"
        ))),
    }
}

fn parse_number_literal(raw: &str) -> Result<Option<PartitionLiteral>> {
    if let Ok(value) = raw.parse::<i32>() {
        return Ok(Some(PartitionLiteral::Int(value)));
    }
    if let Ok(value) = raw.parse::<i64>() {
        return Ok(Some(PartitionLiteral::Long(value)));
    }
    Err(DataFusionError::Plan(format!(
        "INSERT OVERWRITE PARTITION numeric literal `{raw}` is not an integer"
    )))
}

pub(crate) fn bind_partition_field(
    schema: &iceberg::spec::Schema,
    field: &iceberg::spec::PartitionField,
) -> Result<PartitionFieldBinding> {
    let source = schema.field_by_id(field.source_id).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "partition field `{}` source id {} is missing from the table schema",
            field.name, field.source_id
        ))
    })?;
    let primitive_type = nested_primitive(source.as_ref())?;
    Ok(PartitionFieldBinding {
        spec_field_name: field.name.clone(),
        source_column_name: source.name.clone(),
        transform: field.transform,
        primitive_type,
    })
}

fn nested_primitive(field: &NestedField) -> Result<PrimitiveType> {
    match field.field_type.as_ref() {
        Type::Primitive(primitive) => Ok(primitive.clone()),
        other => Err(DataFusionError::Plan(format!(
            "partition source `{}` is not a primitive type (`{other}`)",
            field.name
        ))),
    }
}

pub(crate) fn resolve_binding<'a>(
    bindings: &'a [PartitionFieldBinding],
    name: &str,
) -> Result<&'a PartitionFieldBinding> {
    let mut found: Option<&PartitionFieldBinding> = None;
    for binding in bindings {
        if binding.spec_field_name.eq_ignore_ascii_case(name)
            || (binding.transform == Transform::Identity
                && binding.source_column_name.eq_ignore_ascii_case(name))
        {
            if found.is_some() {
                return Err(DataFusionError::Plan(format!(
                    "INSERT OVERWRITE PARTITION column `{name}` is ambiguous"
                )));
            }
            found = Some(binding);
        }
    }
    found.ok_or_else(|| non_partition_column(name))
}

pub(crate) fn non_partition_column(name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[NON_PARTITION_COLUMN] PARTITION clause cannot contain the non-partition column: \
         `{name}`. SQLSTATE: 42000"
    ))
}

pub(crate) fn equality_predicate(
    binding: &PartitionFieldBinding,
    equality: &PartitionEquality,
) -> Result<Predicate> {
    let reference = Reference::new(binding.source_column_name.clone());
    match &equality.value {
        None => Ok(reference.is_null()),
        Some(literal) => {
            let datum = datum_for_type(
                &binding.primitive_type,
                literal,
                &binding.source_column_name,
            )?;
            Ok(reference.equal_to(datum))
        }
    }
}

pub(crate) fn equality_literal(
    binding: &PartitionFieldBinding,
    equality: &PartitionEquality,
) -> Result<Option<Literal>> {
    match &equality.value {
        None => Ok(None),
        Some(literal) => {
            let datum = datum_for_type(
                &binding.primitive_type,
                literal,
                &binding.source_column_name,
            )?;
            Ok(Some(Literal::from(datum)))
        }
    }
}

fn datum_for_type(
    primitive: &PrimitiveType,
    literal: &PartitionLiteral,
    column: &str,
) -> Result<Datum> {
    match (primitive, literal) {
        (PrimitiveType::Boolean, PartitionLiteral::Boolean(flag)) => Ok(Datum::bool(*flag)),
        (PrimitiveType::Int, PartitionLiteral::Int(value)) => Ok(Datum::int(*value)),
        (PrimitiveType::Int, PartitionLiteral::Long(value)) => {
            let narrowed = i32::try_from(*value).map_err(|_| {
                DataFusionError::Plan(format!(
                    "INSERT OVERWRITE PARTITION value {value} does not fit identity column `{column}` INT"
                ))
            })?;
            Ok(Datum::int(narrowed))
        }
        (PrimitiveType::Long, PartitionLiteral::Long(value)) => Ok(Datum::long(*value)),
        (PrimitiveType::Long, PartitionLiteral::Int(value)) => Ok(Datum::long(i64::from(*value))),
        (PrimitiveType::String, PartitionLiteral::String(text)) => Ok(Datum::string(text)),
        (other, literal) => cast_datum(literal, other, column),
    }
}

fn apply_overwrite_isolation(
    mut action: iceberg::transaction::OverwriteFilesAction,
    isolation: Option<OverwriteIsolation>,
    table: &Table,
    branch: Option<&str>,
) -> iceberg::transaction::OverwriteFilesAction {
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = snapshot_id_for_commit(table, branch) {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    action
}

fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray, StringViewArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::sql::sqlparser::ast::Ident;
    use iceberg::spec::{Schema, Transform, UnboundPartitionSpec};
    use iceberg::{NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    fn ident(name: &str) -> Expr {
        Expr::Identifier(Ident::new(name))
    }

    fn number(raw: &str) -> Expr {
        Expr::Value(ValueWithSpan::from(Value::Number(raw.to_string(), false)))
    }

    fn quoted(text: &str) -> Expr {
        Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
            text.to_string(),
        )))
    }

    fn eq(name: &str, value: Expr) -> Expr {
        Expr::BinaryOp {
            left: Box::new(ident(name)),
            op: BinaryOperator::Eq,
            right: Box::new(value),
        }
    }

    #[test]
    fn store_assign_conforms_a_view_string_source_to_its_utf8_target() {
        let source: ArrayRef = Arc::new(StringViewArray::from(vec!["g", "h"]));
        let field: FieldRef = Arc::new(Field::new("name", DataType::Utf8, true));
        let conformed = store_assign_source_column(&source, &field).expect("conforms");
        assert_eq!(conformed.data_type(), &DataType::Utf8);
        let values = conformed
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 output");
        assert_eq!(values.value(0), "g");
        assert_eq!(values.value(1), "h");
    }

    #[test]
    fn store_assign_is_identity_on_a_match_and_refuses_a_non_assignable_pair() {
        let same: ArrayRef = Arc::new(StringArray::from(vec!["g"]));
        let utf8: FieldRef = Arc::new(Field::new("name", DataType::Utf8, true));
        let conformed = store_assign_source_column(&same, &utf8).expect("identity");
        assert!(Arc::ptr_eq(&same, &conformed));
        let numeric: FieldRef = Arc::new(Field::new("id", DataType::Int32, true));
        let refused = store_assign_source_column(&same, &numeric).expect_err("not assignable");
        assert!(
            refused.to_string().contains("insert overwrite partition"),
            "{refused}"
        );
    }

    #[test]
    fn request_static_and_dynamic_shapes() {
        let static_request =
            partition_overwrite_request_from_exprs(&[eq("id", number("1"))]).expect("static");
        assert_eq!(static_request.equalities.len(), 1);
        assert_eq!(static_request.equalities[0].name, "id");
        assert_eq!(
            static_request.equalities[0].value,
            Some(PartitionLiteral::Int(1))
        );
        assert!(static_request.dynamic_names.is_empty());
        let dynamic =
            partition_overwrite_request_from_exprs(&[ident("id"), ident("cat")]).expect("dynamic");
        assert_eq!(dynamic.dynamic_names, vec!["id".to_string(), "cat".into()]);
        assert!(dynamic.equalities.is_empty());
        let inferred = partition_overwrite_request_from_exprs(&[]).expect("empty");
        assert_eq!(inferred, PartitionOverwriteRequest::default());
        let quoted = partition_overwrite_request_from_exprs(&[quoted("cat")]).expect("quoted");
        assert_eq!(quoted.dynamic_names, vec!["cat".to_string()]);
    }

    #[test]
    fn mixed_static_dynamic_parses() {
        let request =
            partition_overwrite_request_from_exprs(&[eq("id", number("1")), ident("cat")])
                .expect("mix");
        assert_eq!(request.equalities.len(), 1);
        assert_eq!(request.dynamic_names, vec!["cat".to_string()]);
        assert_eq!(request.names, vec!["id".to_string(), "cat".into()]);
    }

    /// String and NULL literals parse.
    #[test]
    fn string_and_null_literals() {
        let null = Expr::Value(ValueWithSpan::from(Value::Null));
        let request =
            partition_overwrite_request_from_exprs(&[eq("cat", quoted("a")), eq("id", null)])
                .expect("literals");
        assert_eq!(
            request.equalities[0].value,
            Some(PartitionLiteral::String("a".into()))
        );
        assert_eq!(request.equalities[1].value, None);
    }

    fn key_payload_batch(keys: &[i32], payloads: &[&str]) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys.to_vec())),
                Arc::new(StringArray::from(payloads.to_vec())),
            ],
        )
        .expect("batch")
    }

    /// pins: dml-b-insert-overwrite/C-001
    #[tokio::test]
    async fn commit_rejects_added_file_outside_overwrite_filter() {
        let warehouse = TempDir::new().expect("warehouse");
        let catalog = crate::catalog::memory_catalog(warehouse.path().to_str().expect("utf8"))
            .await
            .expect("catalog");
        catalog
            .create_namespace(&NamespaceIdent::new("sales".to_string()), HashMap::new())
            .await
            .expect("namespace");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "key", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "payload", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("schema");
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(1, "key", Transform::Identity)
            .expect("partition field")
            .build();
        catalog
            .create_table(
                &NamespaceIdent::new("sales".to_string()),
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .partition_spec(spec)
                    .build(),
            )
            .await
            .expect("create");
        let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "t".to_string());
        crate::write::append(
            &catalog,
            &ident,
            vec![key_payload_batch(&[2], &["outside"])],
        )
        .await
        .expect("seed");
        let table = catalog.load_table(&ident).await.expect("load");
        let files = crate::write::write_partitioned_data_files(
            &table,
            vec![key_payload_batch(&[2], &["outside"])],
        )
        .await
        .expect("stage key=2 file");
        let request =
            partition_overwrite_request_from_exprs(&[eq("key", number("1"))]).expect("static");
        let crate::write::OverwritePlan::RowFilter(spec) =
            crate::write::plan_overwrite(&table, &request, crate::write::OverwriteMode::default())
                .expect("plan")
        else {
            panic!("expected static plan");
        };
        let error = commit_overwrite_by_row_filter(&catalog, &table, files, spec.predicate)
            .await
            .expect_err("added file in key=2 must not commit under key=1 filter");
        let message = error.to_string();
        assert!(
            message.contains("Cannot append file with rows that do not match filter")
                || message.contains("do not match filter"),
            "got {message}"
        );
    }
}
