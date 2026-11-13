use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::error::Result;

use super::super::lineage::project_update_data_batch;

fn project(batch: &RecordBatch, values: &SchemaRef, planned: &SchemaRef) -> Result<RecordBatch> {
    project_update_data_batch(batch, values, planned)
}

fn planned_schema(value_nullable: bool) -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("_file", DataType::Utf8, false),
        Field::new("_pos", DataType::Int64, false),
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int32, value_nullable),
        Field::new("flag", DataType::Utf8, false),
    ]))
}

fn values_schema(value_nullable: bool) -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int32, value_nullable),
        Field::new("flag", DataType::Utf8, true),
    ]))
}

fn raw_batch(planned: &SchemaRef, path: &str, id: i64, value: Option<i32>) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![path])),
        Arc::new(Int64Array::from(vec![0_i64])),
        Arc::new(Int64Array::from(vec![id])),
        Arc::new(Int32Array::from(vec![value])),
        Arc::new(StringArray::from(vec!["upd"])),
    ];
    RecordBatch::try_new(Arc::clone(planned), columns).expect("raw batch on the planned schema")
}

fn register(batches: Vec<RecordBatch>) -> Result<MemTable> {
    let schema = batches[0].schema();
    MemTable::try_new(schema, vec![batches])
}

fn projected_pair(
    values: &SchemaRef,
    planned: &SchemaRef,
    first: Option<i32>,
    second: Option<i32>,
) -> Vec<RecordBatch> {
    vec![
        project(&raw_batch(planned, "a.parquet", 6, first), values, planned).expect("first"),
        project(&raw_batch(planned, "b.parquet", 4, second), values, planned).expect("second"),
    ]
}

#[test]
fn null_free_batch_first_then_a_null_batch_registers_one_values_table() {
    let planned = planned_schema(true);
    let batches = projected_pair(&values_schema(true), &planned, Some(-1), None);
    let schemas = (batches[0].schema(), batches[1].schema());
    let table = register(batches);
    assert!(table.is_ok(), "{:?}", table.err());
    assert_eq!(schemas.0, schemas.1);
    assert!(schemas.0.field(1).is_nullable());
}

#[test]
fn null_batch_first_then_a_null_free_batch_registers_one_values_table() {
    let planned = planned_schema(true);
    let batches = projected_pair(&values_schema(true), &planned, None, Some(-1));
    assert_eq!(batches[0].schema(), batches[1].schema());
    let table = register(batches);
    assert!(table.is_ok(), "{:?}", table.err());
}

#[test]
fn computed_null_into_a_required_field_keeps_one_nullable_schema_for_the_writer() {
    let planned = planned_schema(true);
    let batches = projected_pair(&values_schema(false), &planned, Some(2), None);
    let schemas = (batches[0].schema(), batches[1].schema());
    assert_eq!(batches[1].column(1).null_count(), 1);
    let table = register(batches);
    assert!(table.is_ok(), "{:?}", table.err());
    assert_eq!(schemas.0, schemas.1);
    assert!(schemas.0.field(1).is_nullable());
}

#[test]
fn required_field_planned_non_null_stays_non_nullable_on_every_batch() {
    let planned = planned_schema(false);
    let batches = projected_pair(&values_schema(false), &planned, Some(2), Some(3));
    for batch in &batches {
        assert!(!batch.schema().field(1).is_nullable());
        assert!(batch.schema().field(0).is_nullable());
        assert!(batch.schema().field(2).is_nullable());
    }
    let table = register(batches);
    assert!(table.is_ok(), "{:?}", table.err());
}
