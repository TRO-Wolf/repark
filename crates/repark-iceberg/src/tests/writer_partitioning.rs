use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::DataFusionError;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use crate::write::illegal_argument::IllegalArgumentMarker;
use crate::write::writer_partitioning::{
    SaveTarget, WriterLayout, check_layout_matches_catalog_table, check_layout_matches_table,
    decide_save_target, provided_transforms, save_target_names_table, table_transforms,
};

fn illegal_argument_text(error: &DataFusionError) -> String {
    let DataFusionError::External(inner) = error else {
        panic!("expected an External marker, got {error:?}");
    };
    inner.downcast_ref::<IllegalArgumentMarker>().map_or_else(
        || panic!("expected an IllegalArgumentMarker, got {inner:?}"),
        |marker| marker.0.clone(),
    )
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(ToString::to_string).collect()
}

fn bucketed(num_buckets: i64, bucket_columns: &[&str]) -> WriterLayout {
    WriterLayout {
        num_buckets: Some(num_buckets),
        bucket_columns: strings(bucket_columns),
        ..WriterLayout::default()
    }
}

async fn table_with(
    warehouse: &TempDir,
    fields: &[(&str, Transform)],
) -> (Arc<dyn Catalog>, Table) {
    let path = warehouse.path().to_str().expect("utf-8 path").to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "mem",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("memory catalog"),
    );
    let namespace = NamespaceIdent::new("ns".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "cat", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
        ])
        .build()
        .expect("schema");
    let mut spec = UnboundPartitionSpec::builder();
    for (source, transform) in fields {
        let source_id = schema.field_id_by_name(source).expect("source column");
        let name = format!("{source}_{}", transform.to_string().replace(['[', ']'], ""));
        spec = spec
            .add_partition_field(source_id, name, *transform)
            .expect("partition field");
    }
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .partition_spec(spec.build())
        .build();
    let table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    (catalog, table)
}

#[test]
fn provided_transforms_render_like_spark_partitioning_as_v2() {
    assert!(provided_transforms(&WriterLayout::default()).is_empty());
    assert_eq!(
        provided_transforms(&bucketed(4, &["id"])),
        strings(&["bucket(4, id)"])
    );
    assert_eq!(
        provided_transforms(&bucketed(4, &["id", "data"])),
        strings(&["bucket(4, id, data)"])
    );
    let layout = WriterLayout {
        partition_columns: strings(&["cat"]),
        sort_columns: strings(&["data"]),
        ..bucketed(4, &["id"])
    };
    assert_eq!(
        provided_transforms(&layout),
        strings(&["identity(cat)", "sorted_bucket(id, 4, data)"])
    );
    assert_eq!(
        provided_transforms(&bucketed(2, &["my col", "9"])),
        strings(&["bucket(2, `my col`, `9`)"])
    );
}

#[tokio::test]
async fn table_transforms_render_like_iceberg_spark_table_partitioning() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = table_with(
        &warehouse,
        &[
            ("cat", Transform::Identity),
            ("ts", Transform::Day),
            ("data", Transform::Truncate(3)),
            ("id", Transform::Bucket(4)),
        ],
    )
    .await;
    assert_eq!(
        table_transforms(&table),
        strings(&[
            "identity(cat)",
            "days(ts)",
            "truncate(3, data)",
            "bucket(4, id)"
        ])
    );
}

#[tokio::test]
async fn a_mismatched_layout_refuses_with_spark_requirement_text() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = table_with(
        &warehouse,
        &[
            ("cat", Transform::Identity),
            ("ts", Transform::Day),
            ("data", Transform::Truncate(3)),
        ],
    )
    .await;
    let layout = WriterLayout {
        partition_columns: strings(&["cat"]),
        ..bucketed(4, &["id"])
    };
    let error = check_layout_matches_table(&table, &layout).expect_err("mismatch refuses");
    assert_eq!(
        illegal_argument_text(&error),
        "requirement failed: The provided partitioning or clustering columns do not match the \
         existing table's.\n - provided: identity(cat), bucket(4, id)\n - table: identity(cat), \
         days(ts), truncate(3, data)"
    );
}

#[tokio::test]
async fn an_unpartitioned_table_renders_an_empty_table_side() {
    let warehouse = TempDir::new().expect("tempdir");
    let (_catalog, table) = table_with(&warehouse, &[]).await;
    let error =
        check_layout_matches_table(&table, &bucketed(4, &["id"])).expect_err("mismatch refuses");
    assert_eq!(
        illegal_argument_text(&error),
        "requirement failed: The provided partitioning or clustering columns do not match the \
         existing table's.\n - provided: bucket(4, id)\n - table: "
    );
    check_layout_matches_table(&table, &WriterLayout::default()).expect("no layout passes");
}

#[tokio::test]
async fn a_matching_layout_passes_and_column_case_is_significant() {
    let warehouse = TempDir::new().expect("tempdir");
    let (catalog, table) = table_with(&warehouse, &[("id", Transform::Bucket(4))]).await;
    let ident = TableIdent::new(NamespaceIdent::new("ns".into()), "t".into());
    check_layout_matches_catalog_table(catalog.as_ref(), &ident, &bucketed(4, &["id"]))
        .await
        .expect("matching layout passes");
    let upper = check_layout_matches_table(&table, &bucketed(4, &["ID"])).expect_err("case");
    assert!(
        illegal_argument_text(&upper).ends_with("provided: bucket(4, ID)\n - table: bucket(4, id)")
    );
    let other = check_layout_matches_table(&table, &bucketed(8, &["id"])).expect_err("count");
    assert!(
        illegal_argument_text(&other).ends_with("provided: bucket(8, id)\n - table: bucket(4, id)")
    );
}

fn relation() -> Vec<String> {
    strings(&["u7", "t"])
}

#[test]
fn save_target_maps_mode_and_existence_like_spark_save() {
    let decide = |exists, mode| decide_save_target("sc.u7.t", &relation(), exists, mode, true);
    assert_eq!(decide(true, "append").expect("append"), SaveTarget::Append);
    assert_eq!(
        decide(true, "overwrite").expect("overwrite"),
        SaveTarget::Overwrite
    );
    assert_eq!(decide(true, "ignore").expect("ignore"), SaveTarget::Skip);
    for mode in ["error", "errorifexists", "ignore"] {
        assert_eq!(decide(false, mode).expect("create"), SaveTarget::Create);
    }
    for mode in ["error", "errorifexists"] {
        assert_eq!(
            decide(true, mode).expect_err("exists").to_string(),
            "[TABLE_OR_VIEW_ALREADY_EXISTS] Cannot create table or view `u7`.`t` because it \
             already exists.\nChoose a different name, drop or replace the existing object, or \
             add the IF NOT EXISTS clause to tolerate pre-existing objects. SQLSTATE: 42P07"
        );
    }
    for mode in ["append", "overwrite"] {
        assert!(
            decide(false, mode)
                .expect_err("missing")
                .to_string()
                .starts_with("[TABLE_OR_VIEW_NOT_FOUND] The table or view u7.t cannot be found.")
        );
    }
}

#[test]
fn save_target_path_and_default_format_refusals() {
    let error = decide_save_target("/tmp/wh/pathtbl", &[], false, "append", true)
        .expect_err("path append refuses");
    assert!(
        error
            .to_string()
            .starts_with("[TABLE_OR_VIEW_NOT_FOUND] The table or view `/tmp/wh`.pathtbl cannot")
    );
    let declared = "DataFrameWriter.save(path) requires format('parquet'|'csv'|'json'|'text'); \
                    use saveAsTable for Iceberg tables";
    for (target, mode, explicit) in [
        ("/tmp/wh/pathtbl", "error", true),
        ("/tmp/wh/pathtbl", "overwrite", false),
        ("sc.u7.t", "append", false),
    ] {
        let refusal = decide_save_target(target, &relation(), true, mode, explicit)
            .expect_err("declared refusal");
        assert_eq!(refusal.to_string(), declared, "{target} {mode} {explicit}");
    }
}

#[test]
fn path_relations_split_at_the_last_slash_like_iceberg_path_identifier() {
    for (path, relation) in [
        ("file:///tmp/probe/a/b", "`file:///tmp/probe/a`.b"),
        ("/tmp/probe/a/b/", "`/tmp/probe/a/b`.``"),
        ("/tmp//x//y", "`/tmp//x/`.y"),
        ("s3://b/k/t", "`s3://b/k`.t"),
        ("a/b", "a.b"),
        ("/x", "``.x"),
    ] {
        let error =
            decide_save_target(path, &[], false, "append", true).expect_err("path append refuses");
        assert!(
            error.to_string().starts_with(&format!(
                "[TABLE_OR_VIEW_NOT_FOUND] The table or view {relation} cannot be found."
            )),
            "{path}: {error}"
        );
    }
}

#[test]
fn only_an_explicit_slash_free_target_names_a_table() {
    assert!(save_target_names_table("sc.u7.t", true));
    assert!(!save_target_names_table("sc.u7.t", false));
    assert!(!save_target_names_table("/tmp/t", true));
    assert!(!save_target_names_table("s3://b/t", true));
}
