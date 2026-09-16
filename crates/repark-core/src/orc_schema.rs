use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::logical_expr::{Cast, Expr, col, lit};
use datafusion::prelude::DataFrame;
use datafusion::scalar::ScalarValue;
use orc_rust::ArrowReaderBuilder;
use orc_rust::schema::{DataType as OrcDataType, RootDataType, TimestampPrecision};

use crate::orc_footer::spark_catalyst_types;
use crate::orc_scan::{ORC_TZ_UTC, orc_footer_error, orc_merge_error, orc_schema_error};
use crate::partition_discovery::user_partition_type;
use crate::spark_nullable::relax_schema_to_nullable;
use crate::{Error, Result, engine_err};

fn spark_catalyst_hit(attrs: &HashMap<usize, String>, index: usize, want: &str) -> bool {
    attrs.get(&index).is_some_and(|kind| kind == want)
}

fn spark_orc_type(orc: &OrcDataType, arrow: &DataType, attrs: &HashMap<usize, String>) -> DataType {
    match (orc, arrow) {
        (OrcDataType::Timestamp { .. }, DataType::Timestamp(unit, None)) => {
            DataType::Timestamp(*unit, Some(Arc::from(ORC_TZ_UTC)))
        }
        (OrcDataType::TimestampWithLocalTimezone { .. }, DataType::Timestamp(unit, Some(_))) => {
            DataType::Timestamp(*unit, None)
        }
        (OrcDataType::Long { column_index }, DataType::Int64)
            if spark_catalyst_hit(attrs, *column_index, "timestamp_ntz") =>
        {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        (OrcDataType::Int { column_index }, DataType::Int32)
            if spark_catalyst_hit(attrs, *column_index, "date") =>
        {
            DataType::Date32
        }
        (OrcDataType::Struct { children, .. }, DataType::Struct(fields))
            if children.len() == fields.len() =>
        {
            DataType::Struct(
                children
                    .iter()
                    .zip(fields.iter())
                    .map(|(child, field)| {
                        field.as_ref().clone().with_data_type(spark_orc_type(
                            child.data_type(),
                            field.data_type(),
                            attrs,
                        ))
                    })
                    .collect(),
            )
        }
        (OrcDataType::List { child, .. }, DataType::List(inner)) => DataType::List(Arc::new(
            inner
                .as_ref()
                .clone()
                .with_data_type(spark_orc_type(child, inner.data_type(), attrs)),
        )),
        (OrcDataType::List { child, .. }, DataType::LargeList(inner)) => DataType::LargeList(
            Arc::new(inner.as_ref().clone().with_data_type(spark_orc_type(
                child,
                inner.data_type(),
                attrs,
            ))),
        ),
        (OrcDataType::Map { key, value, .. }, DataType::Map(entries, sorted)) => {
            match entries.data_type() {
                DataType::Struct(pair) if pair.len() == 2 => {
                    let key_field = pair[0].as_ref().clone().with_data_type(spark_orc_type(
                        key,
                        pair[0].data_type(),
                        attrs,
                    ));
                    let value_field = pair[1].as_ref().clone().with_data_type(spark_orc_type(
                        value,
                        pair[1].data_type(),
                        attrs,
                    ));
                    DataType::Map(
                        Arc::new(
                            entries.as_ref().clone().with_data_type(DataType::Struct(
                                vec![key_field, value_field].into(),
                            )),
                        ),
                        *sorted,
                    )
                }
                _ => arrow.clone(),
            }
        }
        _ => arrow.clone(),
    }
}

fn orc_file_parts(path: &Path) -> std::result::Result<(Schema, RootDataType), String> {
    let file = File::open(path).map_err(|error| format!("cannot open: {error}"))?;
    let builder = ArrowReaderBuilder::try_new(file)
        .map_err(|error| format!("{error}"))?
        .with_timestamp_precision(TimestampPrecision::Microsecond);
    let schema = builder.schema().as_ref().clone();
    let root = builder.file_metadata().root_data_type().clone();
    Ok((schema, root))
}

pub(crate) fn normalize_orc_schema(
    schema: &Schema,
    root: &RootDataType,
    attrs: &HashMap<usize, String>,
) -> SchemaRef {
    let mut swapped: Vec<Field> = Vec::with_capacity(schema.fields().len());
    for (field, child) in schema.fields().iter().zip(root.children().iter()) {
        let mapped = spark_orc_type(child.data_type(), field.data_type(), attrs);
        swapped.push(field.as_ref().clone().with_data_type(mapped));
    }
    for field in schema.fields().iter().skip(swapped.len()) {
        swapped.push(field.as_ref().clone());
    }
    let renamed = Schema::new_with_metadata(swapped, schema.metadata().clone());
    Arc::new(relax_schema_to_nullable(&renamed))
}

pub(crate) fn orc_types_equal(first: &DataType, next: &DataType) -> bool {
    if first == next {
        return true;
    }
    match (first, next) {
        (
            DataType::Timestamp(first_unit, first_zone),
            DataType::Timestamp(next_unit, next_zone),
        ) => first_unit == next_unit && first_zone.is_some() == next_zone.is_some(),
        _ => false,
    }
}

fn union_orc_fields(first: &[Field], next: &[Field]) -> Result<Vec<Field>> {
    let mut out: Vec<Field> = first.to_vec();
    let mut index: HashMap<String, usize> = HashMap::new();
    for (position, field) in first.iter().enumerate() {
        index.insert(field.name().to_lowercase(), position);
    }
    for field in next {
        if let Some(position) = index.get(&field.name().to_lowercase()) {
            if !orc_types_equal(out[*position].data_type(), field.data_type()) {
                return Err(orc_merge_error(
                    out[*position].name(),
                    out[*position].data_type(),
                    field.data_type(),
                ));
            }
        } else {
            index.insert(field.name().to_lowercase(), out.len());
            out.push(field.clone());
        }
    }
    Ok(out)
}

pub(crate) fn infer_orc_schema(
    files: &[PathBuf],
    merge_schema: bool,
    ignore_corrupt: bool,
) -> Result<(SchemaRef, Vec<PathBuf>)> {
    let mut merged: Vec<Field> = Vec::new();
    let mut valid: Vec<PathBuf> = Vec::new();
    let mut seeded = false;
    for file in files {
        let (raw, root) = match orc_file_parts(file) {
            Ok(parts) => parts,
            Err(detail) => {
                if ignore_corrupt {
                    continue;
                }
                return Err(orc_footer_error(file, &detail));
            }
        };
        let attrs = spark_catalyst_types(file);
        let normalized = normalize_orc_schema(&raw, &root, &attrs);
        valid.push(file.clone());
        let next: Vec<Field> = normalized
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        if seeded {
            if merge_schema {
                merged = union_orc_fields(&merged, &next)?;
            }
        } else {
            merged = next;
            seeded = true;
        }
    }
    if !seeded {
        return Err(orc_schema_error());
    }
    Ok((Arc::new(Schema::new(merged)), valid))
}

fn is_integer_orc_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

pub(crate) fn apply_user_orc_schema(
    frame: DataFrame,
    user: &[(String, String)],
    data_schema: &SchemaRef,
    partition_fields: &[Field],
    session_zone: &str,
) -> Result<DataFrame> {
    let mut lowered: HashMap<String, &Field> = HashMap::new();
    for field in data_schema.fields() {
        lowered.insert(field.name().to_lowercase(), field);
    }
    for field in partition_fields {
        lowered.entry(field.name().to_lowercase()).or_insert(field);
    }
    let mut exprs: Vec<Expr> = Vec::with_capacity(user.len());
    for (name, kind) in user {
        let key = name.to_lowercase();
        let Some(found) = lowered.get(&key) else {
            let normalized = kind.to_lowercase();
            let Some((data_type, _)) = user_partition_type(normalized.as_str(), session_zone)
            else {
                return Err(Error::Analysis(format!(
                    "orc read cannot apply schema type `{kind}` to missing column `{name}`"
                )));
            };
            exprs.push(
                Expr::Cast(Cast::new(Box::new(lit(ScalarValue::Null)), data_type)).alias(name),
            );
            continue;
        };
        let source = col(found.name());
        let normalized = kind.to_lowercase();
        match user_partition_type(normalized.as_str(), session_zone) {
            Some((data_type, _)) if orc_types_equal(found.data_type(), &data_type) => {
                exprs.push(source.alias(name));
            }
            Some((DataType::Utf8, _)) if is_integer_orc_type(found.data_type()) => {
                exprs.push(Expr::Cast(Cast::new(Box::new(source), DataType::Utf8)).alias(name));
            }
            Some(_) => {
                return Err(Error::Analysis(format!(
                    "orc read cannot convert column `{}` from {} to `{kind}`",
                    found.name(),
                    found.data_type()
                )));
            }
            None => {
                return Err(Error::Analysis(format!(
                    "orc read cannot apply schema type `{kind}` to column `{}` of type {}",
                    found.name(),
                    found.data_type()
                )));
            }
        }
    }
    frame.select(exprs).map_err(engine_err)
}
