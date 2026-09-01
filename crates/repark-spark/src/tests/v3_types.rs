//! Model: Grok 4.6
//! V3-6 type CREATE/ALTER pins.
//! pins: v3-6-v3-types/C-001, C-003

use std::fs;
use std::path::{Path, PathBuf};

use super::super::*;
use super::common::*;

fn find_ledger(dir: &Path, suffix: &str) -> Option<PathBuf> {
    for entry in fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.is_dir() {
            if let Some(found) = find_ledger(&path, suffix) {
                return Some(found);
            }
        } else if path
            .file_name()
            .is_some_and(|name| name.to_string_lossy().ends_with(suffix))
        {
            return Some(path);
        }
    }
    None
}

#[tokio::test]
async fn v3_types_oracle_matrix_is_the_c001_record() {
    let ledgers = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/")
        .parent()
        .expect("repo")
        .join("task/ledgers");
    let ledger = find_ledger(&ledgers, "v3-6-v3-types-ledger.md")
        .expect("the V3-6 ledger lives somewhere under task/ledgers/");
    let text = fs::read_to_string(&ledger).expect("C-001 ledger");
    assert!(
        text.contains("arrow.parquet.variant")
            && text.contains("UNSUPPORTED_DATATYPE")
            && text.contains("cannot visit arrow data type: null")
            && text.contains("setting default values in Spark is currently unsupported")
            && text.contains("missing column"),
        "C-001 matrix must stay in the ledger"
    );
}

#[tokio::test]
async fn engine_create_timestamp_ns_unknown_variant_and_default_today() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    for (table, type_sql) in [("t_unknown", "UNKNOWN"), ("t_variant", "VARIANT")] {
        let err = execute(
            &ctx,
            &catalogs,
            &format!(
                "CREATE TABLE ice.sales.{table} (id INT, v {type_sql}) USING iceberg \
                 TBLPROPERTIES ('format-version' = '3')"
            ),
        )
        .await
        .expect_err("v3 type CREATE must not silently succeed before this unit lands it")
        .to_string();
        assert!(
            !err.is_empty(),
            "{type_sql} CREATE must refuse with a message"
        );
        let exists = catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap();
        assert!(!exists, "refused CREATE must leave no `{table}` behind");
    }

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.plain (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect("plain v3 CREATE");
    let add_default = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.plain ADD COLUMN tag STRING DEFAULT 'x'",
    )
    .await
    .expect_err("ADD COLUMN DEFAULT is not consumed yet");
    let text = add_default.to_string();
    assert!(
        text.to_ascii_lowercase().contains("default")
            || text.to_ascii_lowercase().contains("option")
            || text.to_ascii_lowercase().contains("not supported"),
        "ADD COLUMN DEFAULT must refuse naming the option: {text}"
    );
}

#[tokio::test]
async fn opt_in_v3_create_stores_timestamp_ns_and_round_trips() {
    use datafusion::arrow::array::TimestampNanosecondArray;
    use datafusion::arrow::datatypes::{DataType, TimeUnit};
    use iceberg::spec::{PrimitiveType, Type};

    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tsns (id INT, ts timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect("CREATE timestamp_ns");
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "tsns".to_string(),
        ))
        .await
        .expect("load");
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestampNs)
    );
    let nanos: i64 = 1_704_164_645_123_456_789;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), "tsns".to_string());
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![Some(1)])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(nanos)])),
        ],
    )
    .expect("ns batch");
    repark_iceberg::append(&catalogs["ice"], &ident, vec![batch])
        .await
        .expect("append timestamp_ns");
    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT id, ts FROM ice.sales.tsns ORDER BY id",
    )
    .await
    .expect("SELECT timestamp_ns")
    .collect()
    .await
    .expect("collect");
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

#[tokio::test]
async fn opt_in_v3_create_stores_timestamptz_ns() {
    use iceberg::spec::{PrimitiveType, Type};

    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tstzns (id INT, ts timestamptz_ns) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect("CREATE timestamptz_ns");
    let table = catalogs["ice"]
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "tstzns".to_string(),
        ))
        .await
        .expect("load");
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestamptzNs)
    );
}

#[tokio::test]
async fn timestamp_ns_on_v2_create_refuses() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    let err = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.tsns_v2 (id INT, ts timestamp_ns) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await
    .expect_err("v2 timestamp_ns must refuse");
    let text = err.to_string().to_ascii_lowercase();
    assert!(
        text.contains("timestamp_ns") || text.contains("v3") || text.contains("not supported"),
        "v2 timestamp_ns refusal must name the type or v3: {text}"
    );
}

#[tokio::test]
async fn alter_column_set_default_refuses_naming_the_option() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup_allow_create_format_version_3(&warehouse).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.setdef (id INT) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await
    .expect("plain v3 CREATE");
    let err = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.setdef ALTER COLUMN id SET DEFAULT 3",
    )
    .await
    .expect_err("SET DEFAULT must refuse — no engine surface sets an Iceberg column default");
    assert!(
        err.to_string().contains("DEFAULT"),
        "SET DEFAULT refusal must name the option: {err}"
    );
}
