//! Partition-scoped INSERT OVERWRITE: static row-filter and dynamic replace-partitions.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray, new_null_array,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::{DataFusionError, Result};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Ident, UnaryOperator, Value, ValueWithSpan,
};
use iceberg::Catalog;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{DataFile, Datum, NestedField, PrimitiveType, Transform, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use uuid::Uuid;

use crate::write::merge::OPERATION_ID_PROP;
use crate::write::overwrite::{OverwriteIsolation, parse_overwrite_isolation};

/// Needle for the empty-input dynamic overwrite refusal (Spark engine-side guard).
pub const EMPTY_DYNAMIC_OVERWRITE_NEEDLE: &str =
    "Cannot dynamically overwrite partitions with no data";

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

/// Static assignments versus dynamic names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionOverwriteRequest {
    /// `PARTITION (k=v, …)` — `OverwriteFiles` by row filter.
    Static(Vec<PartitionEquality>),
    /// `PARTITION (k, …)` or empty `PARTITION ()` — `ReplacePartitions`.
    Dynamic(Vec<String>),
}

/// Static overwrite: row filter plus the equalities to inject into the source.
#[derive(Debug, Clone)]
pub struct StaticPartitionOverwrite {
    /// Iceberg row filter (`k = v AND …`).
    pub predicate: Predicate,
    /// Partition equalities, in PARTITION-clause order.
    pub equalities: Vec<PartitionEquality>,
}

/// Resolved commit path after validating the request against the table spec.
#[derive(Debug, Clone)]
pub enum PartitionOverwritePlan {
    /// Identity-field row filter for `overwrite_by_row_filter`.
    Static(StaticPartitionOverwrite),
    /// Dynamic replace of partitions present in the added files.
    Dynamic,
}

/// Parse Hive `PARTITION (…)` expressions into a static or dynamic request.
/// # Errors
/// Mixed static/dynamic items, empty static list after filtering, or unsupported expression shapes.
pub fn partition_overwrite_request_from_exprs(
    expressions: &[Expr],
) -> Result<PartitionOverwriteRequest> {
    if expressions.is_empty() {
        return Ok(PartitionOverwriteRequest::Dynamic(Vec::new()));
    }
    let mut static_items: Vec<PartitionEquality> = Vec::new();
    let mut dynamic_names: Vec<String> = Vec::new();
    for expression in expressions {
        match partition_clause_item(expression)? {
            PartitionClauseItem::Dynamic(name) => dynamic_names.push(name),
            PartitionClauseItem::Static(item) => static_items.push(item),
        }
    }
    match (!static_items.is_empty(), !dynamic_names.is_empty()) {
        (true, true) => Err(DataFusionError::Plan(
            "INSERT OVERWRITE PARTITION cannot mix static assignments (k=v) and dynamic names (k)"
                .to_string(),
        )),
        (true, false) => Ok(PartitionOverwriteRequest::Static(static_items)),
        (false, true) => Ok(PartitionOverwriteRequest::Dynamic(dynamic_names)),
        (false, false) => Ok(PartitionOverwriteRequest::Dynamic(Vec::new())),
    }
}

/// Bind a request to the table's default partition spec.
/// # Errors
/// Unpartitioned target, unknown names, non-identity fields on the static path, type mismatch.
pub fn plan_partition_overwrite(
    table: &Table,
    request: &PartitionOverwriteRequest,
) -> Result<PartitionOverwritePlan> {
    let spec = table.metadata().default_partition_spec();
    if spec.is_unpartitioned() {
        return match request {
            PartitionOverwriteRequest::Dynamic(_) => Ok(PartitionOverwritePlan::Dynamic),
            PartitionOverwriteRequest::Static(_) => Err(DataFusionError::NotImplemented(
                "INSERT OVERWRITE … PARTITION (…) requires a partitioned Iceberg table; \
                 the target is unpartitioned"
                    .to_string(),
            )),
        };
    }
    let schema = table.metadata().current_schema();
    let bindings = spec
        .fields()
        .iter()
        .map(|field| bind_partition_field(schema.as_ref(), field))
        .collect::<Result<Vec<_>>>()?;
    match request {
        PartitionOverwriteRequest::Static(equalities) => {
            let mut predicates: Vec<Predicate> = Vec::with_capacity(equalities.len());
            for equality in equalities {
                let binding = resolve_binding(&bindings, &equality.name).map_err(|_| {
                    DataFusionError::NotImplemented(format!(
                        "INSERT OVERWRITE … PARTITION (…) static assignment `{}` is not an \
                         identity partition field of the target table",
                        equality.name
                    ))
                })?;
                if binding.transform != Transform::Identity {
                    return Err(DataFusionError::NotImplemented(format!(
                        "static INSERT OVERWRITE PARTITION only supports identity partition \
                         fields; `{}` uses {}",
                        binding.spec_field_name, binding.transform
                    )));
                }
                predicates.push(equality_predicate(binding, equality)?);
            }
            let predicate = predicates
                .into_iter()
                .reduce(Predicate::and)
                .ok_or_else(|| {
                    DataFusionError::Plan(
                        "INSERT OVERWRITE PARTITION static form needs at least one k=v assignment"
                            .to_string(),
                    )
                })?;
            Ok(PartitionOverwritePlan::Static(StaticPartitionOverwrite {
                predicate,
                equalities: equalities.clone(),
            }))
        }
        PartitionOverwriteRequest::Dynamic(names) => {
            if !names.is_empty() {
                for name in names {
                    resolve_binding(&bindings, name)?;
                }
            }
            Ok(PartitionOverwritePlan::Dynamic)
        }
    }
}

/// Refuse an empty-input dynamic overwrite before any catalog mutation.
/// # Errors
/// [`DataFusionError::Plan`] naming the Spark engine-side empty-dynamic guard.
pub fn refuse_empty_dynamic_overwrite(staged_files: &[DataFile]) -> Result<()> {
    let total_rows: u64 = staged_files.iter().map(DataFile::record_count).sum();
    if staged_files.is_empty() || total_rows == 0 {
        return Err(DataFusionError::Plan(format!(
            "{EMPTY_DYNAMIC_OVERWRITE_NEEDLE} — an empty dynamic overwrite would wipe every \
             partition (Spark refuses this engine-side)"
        )));
    }
    Ok(())
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
    let expected_source = table_schema.fields().len().saturating_sub(by_source.len());
    if batch.num_columns() > expected_source {
        return Err(DataFusionError::Plan(format!(
            "[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to the target, \
             the reason is too many data columns: table has {} columns, static PARTITION \
             injects {}, source has {}",
            table_schema.fields().len(),
            by_source.len(),
            batch.num_columns()
        )));
    }
    if batch.num_columns() != expected_source {
        return Err(DataFusionError::Plan(format!(
            "[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot write to the target: \
             table has {} columns, static PARTITION injects {}, source has {}",
            table_schema.fields().len(),
            by_source.len(),
            batch.num_columns()
        )));
    }
    let mut source_index = 0usize;
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        if let Some(equality) = by_source.get(&field.name().to_ascii_lowercase()) {
            columns.push(constant_partition_array(
                equality,
                field.data_type(),
                batch.num_rows(),
            )?);
        } else {
            columns.push(batch.column(source_index).clone());
            source_index += 1;
        }
    }
    RecordBatch::try_new(Arc::clone(table_schema), columns).map_err(|error| {
        DataFusionError::Execution(format!(
            "INSERT OVERWRITE PARTITION failed to inject static partition columns: {error}"
        ))
    })
}

/// Stage static-overwrite batches after injecting PARTITION (k=v) columns.
/// # Errors
/// Injection, positional map, or file write failures as [`DataFusionError`].
pub async fn stage_static_partition_overwrite_files(
    table: &Table,
    batches: Vec<RecordBatch>,
    equalities: &[PartitionEquality],
    concurrency: crate::write::concurrency::WriteConcurrency,
) -> Result<Vec<DataFile>> {
    let write_schema: SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(iceberg_err)?,
    );
    let mut injected = Vec::with_capacity(batches.len());
    for batch in batches {
        injected.push(inject_static_partition_columns(
            &batch,
            &write_schema,
            equalities,
            table,
        )?);
    }
    let stream = futures::stream::iter(injected.into_iter().map(Ok));
    crate::write::overwrite::write_overwrite_staged_files_from_stream(
        table,
        stream,
        Vec::new(),
        concurrency,
    )
    .await
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
        (literal, other) => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE PARTITION cannot inject `{literal:?}` as {other}"
        ))),
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
    let isolation = parse_overwrite_isolation(table)?;
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let mut action = tx
        .overwrite_files()
        .overwrite_by_row_filter(predicate)
        .validate_added_files_match_overwrite_filter()
        .add_files(staged_files)
        .set_snapshot_properties(summary);
    action = apply_overwrite_isolation(action, isolation, table);
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(catalog.as_ref()).await.map_err(iceberg_err)
}

/// Commit a dynamic partition overwrite: replace partitions present in `staged_files`.
/// # Errors
/// Isolation parse, empty-input guard, action apply, or catalog commit as [`DataFusionError`].
pub async fn commit_replace_partitions(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    staged_files: Vec<DataFile>,
) -> Result<Table> {
    refuse_empty_dynamic_overwrite(&staged_files)?;
    let isolation = parse_overwrite_isolation(table)?;
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
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
        if let Some(snapshot_id) = table.metadata().current_snapshot_id() {
            action = action.validate_from_snapshot(snapshot_id);
        }
    }
    let tx = action.apply(tx).map_err(iceberg_err)?;
    tx.commit(catalog.as_ref()).await.map_err(iceberg_err)
}

enum PartitionClauseItem {
    Dynamic(String),
    Static(PartitionEquality),
}

struct PartitionFieldBinding {
    spec_field_name: String,
    source_column_name: String,
    transform: Transform,
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
            let value = partition_literal(right)?;
            Ok(PartitionClauseItem::Static(PartitionEquality {
                name,
                value,
            }))
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

fn bind_partition_field(
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

fn resolve_binding<'a>(
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
    found.ok_or_else(|| {
        let names: Vec<&str> = bindings
            .iter()
            .map(|binding| binding.spec_field_name.as_str())
            .collect();
        DataFusionError::Plan(format!(
            "PARTITION column `{name}` is not a partition field of the target table (fields: {})",
            names.join(", ")
        ))
    })
}

fn equality_predicate(
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
        (other, literal) => Err(DataFusionError::Plan(format!(
            "INSERT OVERWRITE PARTITION literal `{literal:?}` is not assignable to `{column}` ({other})"
        ))),
    }
}

fn apply_overwrite_isolation(
    mut action: iceberg::transaction::OverwriteFilesAction,
    isolation: Option<OverwriteIsolation>,
    table: &Table,
) -> iceberg::transaction::OverwriteFilesAction {
    if let Some(level) = isolation {
        action = action.validate_no_conflicting_deletes();
        if level == OverwriteIsolation::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(snapshot_id) = table.metadata().current_snapshot_id() {
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
    use datafusion::sql::sqlparser::ast::Ident;

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

    /// Static k=v and dynamic name parse into the two request arms.
    #[test]
    fn request_static_and_dynamic_shapes() {
        let static_request =
            partition_overwrite_request_from_exprs(&[eq("id", number("1"))]).expect("static");
        match static_request {
            PartitionOverwriteRequest::Static(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(items[0].name, "id");
                assert_eq!(items[0].value, Some(PartitionLiteral::Int(1)));
            }
            PartitionOverwriteRequest::Dynamic(_) => panic!("expected static"),
        }
        let dynamic =
            partition_overwrite_request_from_exprs(&[ident("id"), ident("cat")]).expect("dynamic");
        assert_eq!(
            dynamic,
            PartitionOverwriteRequest::Dynamic(vec!["id".into(), "cat".into()])
        );
        let inferred = partition_overwrite_request_from_exprs(&[]).expect("empty");
        assert_eq!(inferred, PartitionOverwriteRequest::Dynamic(Vec::new()));
        let quoted = partition_overwrite_request_from_exprs(&[quoted("cat")]).expect("quoted");
        assert_eq!(
            quoted,
            PartitionOverwriteRequest::Dynamic(vec!["cat".into()])
        );
    }

    /// Mixed static and dynamic items refuse.
    #[test]
    fn mixed_static_dynamic_refuses() {
        let error = partition_overwrite_request_from_exprs(&[eq("id", number("1")), ident("cat")])
            .expect_err("mix");
        assert!(error.to_string().contains("mix"), "got {error}");
    }

    /// String and NULL literals parse.
    #[test]
    fn string_and_null_literals() {
        let null = Expr::Value(ValueWithSpan::from(Value::Null));
        let request =
            partition_overwrite_request_from_exprs(&[eq("cat", quoted("a")), eq("id", null)])
                .expect("literals");
        match request {
            PartitionOverwriteRequest::Static(items) => {
                assert_eq!(items[0].value, Some(PartitionLiteral::String("a".into())));
                assert_eq!(items[1].value, None);
            }
            PartitionOverwriteRequest::Dynamic(_) => panic!("expected static"),
        }
    }

    /// Empty file list hits the dynamic guard.
    #[test]
    fn empty_dynamic_guard_refuses() {
        let error = refuse_empty_dynamic_overwrite(&[]).expect_err("empty");
        assert!(
            error.to_string().contains(EMPTY_DYNAMIC_OVERWRITE_NEEDLE),
            "got {error}"
        );
    }
}
