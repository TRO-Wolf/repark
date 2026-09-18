use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use iceberg::spec::{FormatVersion, NestedField, PrimitiveType, Schema, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::super::{LineageColumnsTableProvider, memory_catalog};

async fn evolved_v3_table(warehouse: &TempDir, swap: bool) -> iceberg::table::Table {
    let catalog: Arc<dyn Catalog> = memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    let namespace = NamespaceIdent::new("evo".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let names: &[&str] = if swap {
        &["id", "extra", "v"]
    } else {
        &["id", "v"]
    };
    let mut fields =
        vec![NestedField::optional(1, names[0], Type::Primitive(PrimitiveType::Long)).into()];
    let mut arrow_fields = vec![Field::new(names[0], DataType::Int64, true)];
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1_i64, 2]))];
    let values: [[Option<&str>; 2]; 2] = [[Some("a"), Some("b")], [Some("e1"), None]];
    for (offset, name) in names[1..].iter().enumerate() {
        let field_id = i32::try_from(offset).expect("small offset") + 2;
        fields.push(
            NestedField::optional(field_id, *name, Type::Primitive(PrimitiveType::String)).into(),
        );
        arrow_fields.push(Field::new(*name, DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(values[offset].to_vec())));
    }
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .expect("schema at the first write");
    catalog
        .create_table(
            &namespace,
            TableCreation::builder()
                .name("t".to_string())
                .schema(schema)
                .format_version(FormatVersion::V3)
                .build(),
        )
        .await
        .expect("create v3");
    let ident = TableIdent::new(namespace, "t".to_string());
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(arrow_fields)), columns)
        .expect("seed batch");
    let mut table = crate::write::append::append(&catalog, &ident, vec![batch])
        .await
        .expect("append the only data file");
    let renames: &[(&str, &str)] = if swap {
        &[("extra", "tmp"), ("v", "extra"), ("tmp", "v")]
    } else {
        &[]
    };
    if !swap {
        let tx = Transaction::new(&table);
        let action = tx
            .update_schema()
            .add_column("extra", Type::Primitive(PrimitiveType::String));
        table = action
            .apply(tx)
            .expect("apply ADD COLUMN")
            .commit(catalog.as_ref())
            .await
            .expect("commit ADD COLUMN");
    }
    for (from, to) in renames {
        let tx = Transaction::new(&table);
        let action = tx.update_schema().rename_column(from, to);
        table = action
            .apply(tx)
            .expect("apply RENAME COLUMN")
            .commit(catalog.as_ref())
            .await
            .expect("commit RENAME COLUMN");
    }
    table
}

async fn lineage_rows(table: iceberg::table::Table, sql: &str) -> Vec<Vec<String>> {
    let provider = LineageColumnsTableProvider::try_new(table).expect("provider");
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider))
        .expect("register");
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("the lineage read runs under the current schema");
    let mut rows = Vec::new();
    for batch in &batches {
        for index in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.is_null(index) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(column, index).expect("display")
                        }
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

fn expected(rows: &[[&str; 4]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|value| (*value).to_string()).collect())
        .collect()
}

#[tokio::test]
async fn row_id_read_after_add_column_null_fills_the_added_column() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_v3_table(&warehouse, false).await;
    assert_eq!(
        lineage_rows(table, "SELECT _row_id, id, v, extra FROM t").await,
        expected(&[["0", "1", "a", "NULL"], ["1", "2", "b", "NULL"]])
    );
}

#[tokio::test]
async fn row_id_read_after_swapping_two_names_reads_each_field_by_id() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = evolved_v3_table(&warehouse, true).await;
    assert_eq!(
        lineage_rows(table.clone(), "SELECT _row_id, id, v, extra FROM t").await,
        expected(&[["0", "1", "a", "e1"], ["1", "2", "b", "NULL"]])
    );
    assert_eq!(
        lineage_rows(table, "SELECT _row_id, id, v, extra FROM t WHERE v = 'a'").await,
        expected(&[["0", "1", "a", "e1"]])
    );
}
