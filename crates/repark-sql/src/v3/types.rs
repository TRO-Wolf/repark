//! Model: Claude Fable 5
//! ANSI-door v3 type CREATE pins.
//! pins: v3r-1-rulings/C-008, C-009
//! pins: v3-6-v3-types/C-003

use iceberg::{NamespaceIdent, TableIdent};

use super::cow::door_with_v3_opt_in;

#[tokio::test]
async fn v3_type_columns_geometry_geography_variant_refuse_naming_the_type() {
    let door = door_with_v3_opt_in().await;
    for type_name in ["GEOMETRY", "GEOGRAPHY", "VARIANT"] {
        let table = format!("t_{}", type_name.to_ascii_lowercase());
        let err = door
            .err(&format!(
                "CREATE TABLE ice.sales.{table} (id INT, v {type_name}) WITH (format_version = 3)"
            ))
            .await;
        assert!(
            err.to_ascii_uppercase().contains(type_name),
            "CREATE with a `{type_name}` column must refuse naming the type: {err}"
        );
        let exists = door
            .catalog
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.clone(),
            ))
            .await
            .expect("table_exists");
        assert!(!exists, "a refused CREATE must leave no `{table}` behind");
    }
}

#[tokio::test]
async fn opt_in_v3_create_stores_timestamp_ns() {
    use iceberg::spec::{PrimitiveType, Type};

    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.tsns (id INT, ts timestamp_ns) WITH (format_version = 3)")
        .await;
    let table = door
        .catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "tsns".to_string(),
        ))
        .await
        .expect("load tsns");
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestampNs)
    );
}

#[tokio::test]
async fn opt_in_v3_create_stores_timestamptz_ns() {
    use iceberg::spec::{PrimitiveType, Type};

    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.tstzns (id INT, ts timestamptz_ns) WITH (format_version = 3)")
        .await;
    let table = door
        .catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "tstzns".to_string(),
        ))
        .await
        .expect("load tstzns");
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestamptzNs)
    );
}

#[tokio::test]
async fn opt_in_v3_timestamp_ns_select_round_trips_ns_values() {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};
    use iceberg::spec::{PrimitiveType, Type};

    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.tsnsrt (id INT, ts timestamp_ns) WITH (format_version = 3)")
        .await;
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "tsnsrt".to_string(),
    );
    let table = door.catalog.load_table(&ident).await.expect("load tsnsrt");
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestampNs)
    );
    let nanos: i64 = 1_704_164_645_123_456_789;
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(nanos)])),
        ],
    )
    .expect("ns batch");
    repark_iceberg::append(&door.catalog, &ident, vec![batch])
        .await
        .expect("append timestamp_ns");
    let batches = door
        .sql("SELECT id, ts FROM ice.sales.tsnsrt ORDER BY id")
        .await
        .expect("SELECT timestamp_ns");
    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    let ts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .expect("ns array");
    assert_eq!(ts.value(0), nanos);
}
