use std::cell::Cell;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, RecordBatchReader};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use arrow::error::ArrowError;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use pyo3::Python;
use tokio::runtime::Runtime;

use crate::dataframe::STREAM_POLL_NO_DETACH;
use crate::fence::fence_stream_poll;

const MAX_NESTED_TYPE_DEPTH: usize = 32;

pub(crate) struct StreamingBatchReader {
    runtime: Arc<Runtime>,
    stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl StreamingBatchReader {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Self {
        Self {
            runtime,
            stream,
            schema,
        }
    }
}

impl Iterator for StreamingBatchReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let Self {
            runtime, stream, ..
        } = self;
        let schema = Arc::clone(&self.schema);
        fence_stream_poll("PyDataFrame.__arrow_c_stream__.next", || {
            let no_detach = STREAM_POLL_NO_DETACH.with(Cell::get);
            let polled = if no_detach {
                runtime.block_on(stream.next())
            } else {
                Python::attach(|python| python.detach(|| runtime.block_on(stream.next())))
            };
            polled.map(|batch| {
                batch
                    .map_err(|error| ArrowError::ExternalError(Box::new(error)))
                    .and_then(|batch| coerce_batch_views(&batch, &schema))
            })
        })
    }
}

impl RecordBatchReader for StreamingBatchReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

pub(crate) fn coerced_export_schema(schema: &SchemaRef) -> SchemaRef {
    if !schema_contains_views(schema) {
        return Arc::clone(schema);
    }
    Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|field| coerce_field_views(field, 0))
            .collect::<Vec<Field>>(),
        schema.metadata().clone(),
    ))
}

fn coerce_batch_views(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let target = schema.fields();
    if batch.num_columns() == target.len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(target.iter())
            .all(|(current, want)| current.data_type() == want.data_type())
    {
        return Ok(batch.clone());
    }
    let columns = batch
        .columns()
        .iter()
        .zip(target.iter())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(Arc::clone(column))
            } else {
                cast(column, field.data_type())
            }
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns)
}

fn schema_contains_views(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| type_contains_views(field.data_type(), 0))
}

fn type_contains_views(data_type: &DataType, depth: usize) -> bool {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return false;
    }
    let child = depth + 1;
    match data_type {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| type_contains_views(field.data_type(), child)),
        DataType::List(inner)
        | DataType::LargeList(inner)
        | DataType::ListView(inner)
        | DataType::LargeListView(inner)
        | DataType::FixedSizeList(inner, _) => type_contains_views(inner.data_type(), child),
        DataType::Map(entries, _) => type_contains_views(entries.data_type(), child),
        _ => false,
    }
}

fn coerce_field_views(field: &Field, depth: usize) -> Field {
    if depth > MAX_NESTED_TYPE_DEPTH {
        return field.clone();
    }
    field
        .clone()
        .with_data_type(coerce_type_views(field.data_type(), depth))
}

fn coerce_type_views(data_type: &DataType, depth: usize) -> DataType {
    let child = depth + 1;
    match data_type {
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|field| coerce_field_views(field, child))
                .collect::<Vec<Field>>(),
        )),
        DataType::List(inner) => DataType::List(Arc::new(coerce_field_views(inner, child))),
        DataType::LargeList(inner) => {
            DataType::LargeList(Arc::new(coerce_field_views(inner, child)))
        }
        DataType::FixedSizeList(inner, size) => {
            DataType::FixedSizeList(Arc::new(coerce_field_views(inner, child)), *size)
        }
        DataType::ListView(inner) => DataType::ListView(Arc::new(coerce_field_views(inner, child))),
        DataType::LargeListView(inner) => {
            DataType::LargeListView(Arc::new(coerce_field_views(inner, child)))
        }
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(pair) if pair.len() >= 2 => {
                let key = coerce_field_views(pair[0].as_ref(), child);
                let value = coerce_field_views(pair[1].as_ref(), child);
                let rebuilt = entries
                    .as_ref()
                    .clone()
                    .with_data_type(DataType::Struct(Fields::from(vec![key, value])));
                DataType::Map(Arc::new(rebuilt), *sorted)
            }
            _ => data_type.clone(),
        },
        _ => data_type.clone(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{BinaryViewArray, StringViewArray};

    use super::*;

    fn view_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8View, true),
            Field::new("raw", DataType::BinaryView, false),
            Field::new("unit", DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringViewArray::from(vec![Some("a"), None, Some("bb")])),
                Arc::new(BinaryViewArray::from(vec![
                    Some(b"x".as_slice()),
                    Some(b"yy".as_slice()),
                    Some(b"z".as_slice()),
                ])),
                Arc::new(arrow::array::Int32Array::from(vec![Some(1), None, Some(3)])),
            ],
        )
        .expect("view batch builds")
    }

    #[test]
    fn schema_without_views_is_shared_unchanged() {
        let schema = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int32, true)]));
        let coerced = coerced_export_schema(&schema);
        assert!(Arc::ptr_eq(&schema, &coerced));
    }

    #[test]
    fn schema_with_views_coerces_flat_and_nested() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8View, true),
            Field::new(
                "top",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::Utf8View,
                    false,
                )])),
                true,
            ),
        ]));
        let coerced = coerced_export_schema(&schema);
        assert_eq!(
            coerced
                .field_with_name("id")
                .expect("id present")
                .data_type(),
            &DataType::Utf8
        );
        assert!(
            coerced
                .field_with_name("id")
                .expect("id present")
                .is_nullable()
        );
        let DataType::Struct(top) = coerced
            .field_with_name("top")
            .expect("top present")
            .data_type()
        else {
            panic!("top stays a struct");
        };
        assert_eq!(top[0].data_type(), &DataType::Utf8);
        assert!(!top[0].is_nullable());
    }

    #[test]
    fn batch_with_views_coerces_values_and_nulls() {
        let batch = view_batch();
        let schema = coerced_export_schema(&batch.schema());
        let coerced = coerce_batch_views(&batch, &schema).expect("coerce succeeds");
        assert_eq!(coerced.num_rows(), 3);
        assert_eq!(
            coerced
                .schema()
                .field_with_name("id")
                .expect("id present")
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            coerced
                .schema()
                .field_with_name("raw")
                .expect("raw present")
                .data_type(),
            &DataType::Binary
        );
        let ids = coerced
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("id column is StringArray");
        assert_eq!(ids.null_count(), 1);
        assert_eq!(
            ids.iter().collect::<Vec<_>>(),
            vec![Some("a"), None, Some("bb")]
        );
        let raws = coerced
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .expect("raw column is BinaryArray");
        assert_eq!(
            raws.iter().collect::<Vec<_>>(),
            vec![
                Some(b"x".as_slice()),
                Some(b"yy".as_slice()),
                Some(b"z".as_slice())
            ]
        );
        let units = coerced
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .expect("unit column passes through");
        assert_eq!(units.null_count(), 1);
    }

    #[test]
    fn batch_without_views_is_returned_unrebuilt() {
        let schema = Arc::new(Schema::new(vec![Field::new("unit", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![
                Some(1),
                None,
            ]))],
        )
        .expect("plain batch builds");
        let coerced = coerce_batch_views(&batch, &schema).expect("fast path succeeds");
        assert!(Arc::ptr_eq(coerced.column(0), batch.column(0)));
        assert_eq!(coerced.schema().fields(), batch.schema().fields());
    }
}
