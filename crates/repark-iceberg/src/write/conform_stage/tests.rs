use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
use datafusion::prelude::SessionContext;
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, StructType, Transform, Type,
    UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::{
    StageRequest, StageSite, clear_stage_record, conform_stage, install_stage_record,
    last_stage_sites, table_targets, zone_partial,
};
use crate::write::concurrency::WriteConcurrency;
use crate::write::merge::session_staging::write_new_data_files_from_stream_with;
use crate::write::struct_pairing::StructPairing;
use crate::write::write_options::WriterStagingOverrides;

fn instant() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("+00:00")))
}

fn wall() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn structure(fields: Vec<(&str, DataType)>) -> DataType {
    DataType::Struct(Fields::from(
        fields
            .into_iter()
            .map(|(name, data_type)| Field::new(name, data_type, true))
            .collect::<Vec<_>>(),
    ))
}

fn list(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("element", element, true)))
}

#[test]
fn the_partial_type_converts_only_wall_date_and_instant_leaves() {
    let target = structure(vec![
        ("a", instant()),
        ("b", DataType::Int64),
        ("c", wall()),
    ]);
    let source = structure(vec![
        ("a", DataType::Date32),
        ("b", DataType::Int32),
        ("c", instant()),
    ]);
    assert_eq!(
        zone_partial(&source, &target),
        Some(structure(vec![
            ("a", instant()),
            ("b", DataType::Int32),
            ("c", wall())
        ]))
    );
    let wall_source = structure(vec![("a", wall()), ("b", DataType::Int64), ("c", wall())]);
    assert_eq!(
        zone_partial(&wall_source, &target),
        Some(structure(vec![
            ("a", instant()),
            ("b", DataType::Int64),
            ("c", wall())
        ]))
    );
}

#[test]
fn a_text_leaf_never_enters_the_seam() {
    assert_eq!(zone_partial(&list(DataType::Utf8), &list(instant())), None);
    assert_eq!(zone_partial(&list(DataType::Utf8), &list(wall())), None);
    let mixed = structure(vec![("a", DataType::Utf8), ("b", DataType::Date32)]);
    let target = structure(vec![("a", instant()), ("b", instant())]);
    assert_eq!(
        zone_partial(&mixed, &target),
        Some(structure(vec![("a", DataType::Utf8), ("b", instant())]))
    );
}

#[test]
fn an_already_stored_or_top_level_value_has_no_partial_type() {
    assert_eq!(zone_partial(&list(instant()), &list(instant())), None);
    assert_eq!(zone_partial(&list(wall()), &list(wall())), None);
    assert_eq!(zone_partial(&wall(), &instant()), Some(instant()));
    let short = structure(vec![("a", wall())]);
    let long = structure(vec![("a", instant()), ("b", instant())]);
    assert_eq!(zone_partial(&short, &long), None);
}

#[test]
fn a_map_value_converts_and_its_key_keeps_its_field() {
    let entries = |value: DataType| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", DataType::Utf8, false),
                    Field::new("values", value, true),
                ])),
                false,
            )),
            false,
        )
    };
    assert_eq!(
        zone_partial(&entries(DataType::Date32), &entries(instant())),
        Some(entries(instant()))
    );
    assert_eq!(
        zone_partial(&entries(DataType::Utf8), &entries(instant())),
        None
    );
}

#[test]
fn table_targets_resolve_listed_names_and_refuse_an_unknown_column() {
    let arrow = ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("Ts", instant(), true),
        Field::new("p", DataType::Utf8, true),
    ]);
    let all = table_targets(&arrow, &[], &["P".to_string()]).expect("targets");
    assert_eq!(
        all,
        vec![
            ("id".to_string(), DataType::Int32),
            ("Ts".to_string(), instant())
        ]
    );
    let listed = table_targets(&arrow, &["ts".to_string()], &[]).expect("targets");
    assert_eq!(listed, vec![("Ts".to_string(), instant())]);
    let refusal = table_targets(&arrow, &["missing".to_string()], &[])
        .expect_err("an unknown column refuses");
    assert!(
        refusal
            .to_string()
            .contains("the write names column `missing`"),
        "{refusal}"
    );
}

#[tokio::test]
async fn a_nested_zone_pair_without_the_seam_refuses_and_records_nothing() {
    let ctx = SessionContext::new();
    install_stage_record(&ctx);
    let frame = ctx
        .sql("SELECT named_struct('a', DATE '2026-01-02') AS v")
        .await
        .expect("frame");
    let targets = vec![("v".to_string(), structure(vec![("a", instant())]))];
    let request = StageRequest {
        site: StageSite::InsertByName,
        targets: &targets,
        by_name: true,
        fold: true,
        gate_op: None,
        pairing: StructPairing::by_name(false),
        table: "``",
    };
    let refusal = conform_stage(&ctx, frame.logical_plan().clone(), &request)
        .expect_err("a missing seam refuses");
    assert!(
        refusal
            .to_string()
            .contains("the store kernel `__repark_zone_seam__` is not registered"),
        "{refusal}"
    );
    assert!(
        refusal
            .to_string()
            .contains("instead of storing the value as UTC")
    );
    assert!(last_stage_sites(&ctx).is_empty());
}

#[tokio::test]
async fn the_stage_record_holds_each_site_once_until_cleared() {
    let ctx = SessionContext::new();
    let frame = ctx.sql("SELECT 1 AS id").await.expect("frame");
    let targets = vec![("id".to_string(), DataType::Int64)];
    let request = StageRequest {
        site: StageSite::MicroBatchAppend,
        targets: &targets,
        by_name: true,
        fold: true,
        gate_op: None,
        pairing: StructPairing::by_name(false),
        table: "``",
    };
    conform_stage(&ctx, frame.logical_plan().clone(), &request).expect("unrecorded stage");
    assert!(last_stage_sites(&ctx).is_empty());
    install_stage_record(&ctx);
    for _ in 0..2 {
        conform_stage(&ctx, frame.logical_plan().clone(), &request).expect("stage");
    }
    assert_eq!(last_stage_sites(&ctx), vec![StageSite::MicroBatchAppend]);
    clear_stage_record(&ctx);
    assert!(last_stage_sites(&ctx).is_empty());
}

async fn staging_table(
    warehouse: &TempDir,
    name: &str,
    partitioned: bool,
    version: FormatVersion,
) -> Table {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse")
        .to_string();
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load(
            "mem",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
        )
        .await
        .expect("memory catalog");
    let namespace = NamespaceIdent::new("ns".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let pair = StructType::new(vec![
        NestedField::optional(4, "a", Type::Primitive(PrimitiveType::Int)).into(),
        NestedField::optional(5, "b", Type::Primitive(PrimitiveType::Int)).into(),
    ]);
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "part", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "st", Type::Struct(pair)).into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .format_version(version);
    let creation = if partitioned {
        let spec = UnboundPartitionSpec::builder()
            .add_partition_field(2, "part", Transform::Identity)
            .expect("identity partition field")
            .build();
        creation.partition_spec(spec).build()
    } else {
        creation.build()
    };
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    catalog
        .load_table(&TableIdent::new(namespace, name.into()))
        .await
        .expect("load")
}

fn swapped_batch() -> RecordBatch {
    let fields = Fields::from(vec![
        Field::new("b", DataType::Int32, true),
        Field::new("a", DataType::Int32, true),
    ]);
    let st = StructArray::new(
        fields.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("part", DataType::Int32, false),
        Field::new("st", DataType::Struct(fields), true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            Arc::new(st) as ArrayRef,
        ],
    )
    .expect("batch")
}

#[tokio::test]
async fn the_staging_sink_pairs_on_every_branch() {
    for (name, partitioned, version) in [
        ("flat", false, FormatVersion::V2),
        ("parted", true, FormatVersion::V2),
        ("lineage", true, FormatVersion::V3),
    ] {
        let warehouse = TempDir::new().expect("warehouse");
        let table = staging_table(&warehouse, name, partitioned, version).await;
        let write_schema = Arc::new(
            iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
                .expect("arrow schema"),
        );
        let stream = futures::stream::iter(vec![Ok(swapped_batch())]);
        let refusal = write_new_data_files_from_stream_with(
            &table,
            &write_schema,
            stream,
            WriteConcurrency::default(),
            &WriterStagingOverrides::none(),
        )
        .await
        .expect_err("an unpaired struct refuses on every branch");
        assert!(
            refusal
                .to_string()
                .contains("[INCOMPATIBLE_DATA_FOR_TABLE."),
            "{name}: {refusal}"
        );
    }
}
