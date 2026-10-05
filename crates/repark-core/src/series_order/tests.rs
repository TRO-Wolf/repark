use std::sync::Arc;

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::Column;
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn::sum;
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Expr, SortExpr, col, lit};
use datafusion::prelude::{DataFrame, SessionContext};

use super::{
    SeriesOrderNotice, SeriesOrderSource, claim_series_order_notice, first_temporal_key,
    resolve_series_order,
};

fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
    Arc::new(Schema::new(
        fields
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect::<Vec<_>>(),
    ))
}

fn column(data_type: &DataType, rows: i64) -> ArrayRef {
    match data_type {
        DataType::Date32 => Arc::new(Date32Array::from_iter_values(
            (0..rows).map(|row| i32::try_from(row).unwrap_or_default()),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from_iter_values(
                (0..rows).map(|row| 1_000_000 * (rows - row)),
            ))
        }
        DataType::Int64 => Arc::new(Int64Array::from_iter_values(0..rows)),
        _ => Arc::new(Float64Array::from_iter_values(
            (0..rows).map(|row| f64::from(u32::try_from(row).unwrap_or_default())),
        )),
    }
}

fn table(fields: &[(&str, DataType)], sort_order: Option<Vec<SortExpr>>) -> DataFrame {
    let schema = schema(fields);
    let columns = fields
        .iter()
        .map(|(_, data_type)| column(data_type, 8))
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), columns).expect("probe batch");
    let mut memtable = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    if let Some(order) = sort_order {
        memtable = memtable.with_sort_order(vec![order]);
    }
    SessionContext::new()
        .read_table(Arc::new(memtable))
        .expect("table frame")
}

fn ts() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn wide() -> Vec<(&'static str, DataType)> {
    vec![
        ("id", DataType::Int64),
        ("day", DataType::Date32),
        ("ts1", ts()),
        ("ts2", ts()),
        ("close", DataType::Float64),
    ]
}

fn resolve(frame: &DataFrame) -> super::SeriesOrder {
    resolve_series_order(frame.logical_plan(), frame.schema())
}

fn key_name(order: &super::SeriesOrder) -> Vec<(String, bool, bool)> {
    order
        .keys
        .iter()
        .map(|key| match &key.expr {
            Expr::Column(column) => (column.name.clone(), key.asc, key.nulls_first),
            other => (other.to_string(), key.asc, key.nulls_first),
        })
        .collect()
}

#[test]
fn series_order_declared_beats_temporal() {
    let sorted = table(&wide(), None)
        .sort(vec![col("ts2").alias("ts2").sort(false, false)])
        .expect("sort");
    let order = resolve(&sorted);
    assert_eq!(order.source, SeriesOrderSource::Declared);
    assert_eq!(key_name(&order), vec![("ts2".to_owned(), false, false)]);

    let carried = sorted
        .clone()
        .filter(col("close").gt(lit(1.0)))
        .expect("filter")
        .select(vec![col("close"), col("ts2").alias("when"), col("ts1")])
        .expect("select")
        .limit(0, Some(5))
        .expect("limit")
        .alias("sub")
        .expect("alias");
    let order = resolve(&carried);
    assert_eq!(order.source, SeriesOrderSource::Declared);
    assert_eq!(key_name(&order), vec![("when".to_owned(), false, false)]);
    let Expr::Column(Column { relation, .. }) = &order.keys[0].expr else {
        panic!("a column key");
    };
    assert_eq!(
        relation.as_ref().map(ToString::to_string),
        Some("sub".to_owned())
    );

    let windowed = sorted
        .window(vec![row_number().alias("rn")])
        .expect("window frame");
    assert_eq!(resolve(&windowed).source, SeriesOrderSource::Declared);

    let declared = table(
        &wide(),
        Some(vec![SortExpr::new(
            Expr::Column(Column::from_name("ts2")),
            true,
            false,
        )]),
    );
    let order = resolve(&declared);
    assert_eq!(order.source, SeriesOrderSource::Declared);
    assert_eq!(key_name(&order), vec![("ts2".to_owned(), true, false)]);
    let projected = declared
        .select(vec![col("close"), col("ts2")])
        .expect("projection");
    assert_eq!(
        key_name(&resolve(&projected)),
        vec![("ts2".to_owned(), true, false)]
    );
}

#[test]
fn series_order_declared_lost_falls_through() {
    let sorted = table(&wide(), None)
        .sort(vec![col("ts2").sort(true, true)])
        .expect("sort");
    let dropped = sorted
        .clone()
        .select(vec![col("close"), col("ts1")])
        .expect("drop the key");
    assert_eq!(
        resolve(&dropped).source,
        SeriesOrderSource::FirstTemporal("ts1".to_owned())
    );
    let computed = table(&wide(), None)
        .sort(vec![(col("close") + lit(1.0)).sort(true, true)])
        .expect("expression sort");
    assert_eq!(
        resolve(&computed).source,
        SeriesOrderSource::FirstTemporal("ts1".to_owned())
    );
    let aggregated = sorted
        .aggregate(vec![col("ts2")], vec![sum(col("close")).alias("total")])
        .expect("aggregate");
    assert_eq!(
        resolve(&aggregated).source,
        SeriesOrderSource::FirstTemporal("ts2".to_owned())
    );
}

#[test]
fn series_order_timestamp_before_date() {
    let frame = table(&wide(), None);
    let order = resolve(&frame);
    assert_eq!(
        order.source,
        SeriesOrderSource::FirstTemporal("ts1".to_owned())
    );
    assert_eq!(key_name(&order), vec![("ts1".to_owned(), true, true)]);
    let zoned = schema(&[
        ("day", DataType::Date32),
        ("close", DataType::Float64),
        (
            "at",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        ),
    ]);
    let dfschema =
        datafusion::common::DFSchema::try_from(zoned.as_ref().clone()).expect("dfschema");
    let key = first_temporal_key(&dfschema).expect("a temporal key");
    assert_eq!(key.expr, Expr::Column(Column::new_unqualified("at")));
}

#[test]
fn series_order_date_fallback() {
    let frame = table(
        &[
            ("close", DataType::Float64),
            ("day", DataType::Date32),
            ("later", DataType::Date32),
        ],
        None,
    );
    let order = resolve(&frame);
    assert_eq!(
        order.source,
        SeriesOrderSource::FirstTemporal("day".to_owned())
    );
    assert_eq!(key_name(&order), vec![("day".to_owned(), true, true)]);
}

#[test]
fn series_order_current_row_order_without_temporal() {
    let frame = table(
        &[("id", DataType::Int64), ("close", DataType::Float64)],
        None,
    );
    let order = resolve(&frame);
    assert_eq!(order.source, SeriesOrderSource::CurrentRowOrder);
    assert!(order.keys.is_empty());
}

#[test]
fn series_order_notice_claims_once_per_session() {
    let config = super::with_series_order_notice(datafusion::prelude::SessionConfig::new());
    let cloned = config.clone();
    assert!(claim_series_order_notice(config.options()));
    assert!(!claim_series_order_notice(cloned.options()));
    assert!(!claim_series_order_notice(config.options()));
    let other = super::with_series_order_notice(datafusion::prelude::SessionConfig::new());
    assert!(claim_series_order_notice(other.options()));
    let mut options = other.options().as_ref().clone();
    assert!(options.set("repark.series.issued", "false").is_err());
    assert!(options.extensions.get::<SeriesOrderNotice>().is_some());
    let bare = datafusion::prelude::SessionConfig::new();
    assert!(claim_series_order_notice(bare.options()));
    assert!(claim_series_order_notice(bare.options()));
}
