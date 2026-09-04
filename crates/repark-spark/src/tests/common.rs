use super::super::*;

// Shared imports keep leaf test modules concise; unused names are intentional re-exports.
#[allow(unused_imports)]
pub(super) use std::collections::HashMap;
#[allow(unused_imports)]
pub(super) use std::sync::Arc;

#[allow(unused_imports)]
pub(super) use datafusion::arrow::array::{
    Array, Int32Array, Int64Array, RecordBatch, StringArray,
};
#[allow(unused_imports)]
pub(super) use datafusion::arrow::datatypes::{DataType, Field, Schema};
#[allow(unused_imports)]
pub(super) use datafusion::error::DataFusionError;
#[allow(unused_imports)]
pub(super) use datafusion::prelude::SessionContext;
#[allow(unused_imports)]
pub(super) use datafusion::sql::sqlparser::ast::Statement;
#[allow(unused_imports)]
pub(super) use datafusion::sql::sqlparser::dialect::DatabricksDialect;
#[allow(unused_imports)]
pub(super) use datafusion::sql::sqlparser::parser::Parser;
#[allow(unused_imports)]
pub(super) use iceberg::Catalog;
#[allow(unused_imports)]
pub(super) use iceberg::CatalogBuilder;
#[allow(unused_imports)]
pub(super) use iceberg::io::LocalFsStorageFactory;
#[allow(unused_imports)]
pub(super) use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
#[allow(unused_imports)]
pub(super) use iceberg::{NamespaceIdent, TableCreation, TableIdent};
#[allow(unused_imports)]
pub(super) use repark_core::{CatalogRegistry, LocationPolicy};
#[allow(unused_imports)]
pub(super) use tempfile::TempDir;

/// A `SessionContext` with in-memory Iceberg catalog `ice`, temp view `src`, and `CatalogRegistry`.
pub(super) async fn setup(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    setup_with_ansi(wh, true).await
}

/// Like [`setup`] with an explicit `spark.sql.ansi.enabled` (U5 knob-state twins).
pub(super) async fn setup_with_ansi(
    wh: &TempDir,
    ansi_enabled: bool,
) -> (SessionContext, CatalogRegistry) {
    setup_with_sql_settings(
        wh,
        ansi_enabled,
        repark_functions::cardinality::ReparkSqlSettings::default(),
    )
    .await
}

/// Like [`setup`] with explicit `repark.sql.*` settings (V3-2 / SEC-02 fixtures).
pub(super) async fn setup_with_sql_settings(
    wh: &TempDir,
    ansi_enabled: bool,
    settings: repark_functions::cardinality::ReparkSqlSettings,
) -> (SessionContext, CatalogRegistry) {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .unwrap(),
    );
    let ns_props = HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]);
    catalog
        .create_namespace(&NamespaceIdent::new("sales".to_string()), ns_props)
        .await
        .unwrap();
    let config = repark_functions::cardinality::with_repark_sql_config(
        crate::extension::apply_spark_float_as_decimal(datafusion::prelude::SessionConfig::new()),
        settings,
    );
    let config = repark_functions::ansi::with_spark_ansi_config(config, ansi_enabled);
    let ctx = SessionContext::new_with_config(config);
    // Production wiring: repark-session installs the Spark analyzer rules on every context.
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", catalog.clone())
        .await
        .unwrap();
    register_source(&ctx, "src", &[(1, "a"), (2, "b"), (3, "c")]);

    let mut catalogs = CatalogRegistry::from([("ice".to_string(), catalog)]);
    // SEC-02 grandfather: warehouse root (memory catalog path).
    catalogs.note_local_warehouse_root(warehouse);
    (ctx, catalogs)
}

/// pins: v3-2-create-v3-opt-in/C-005
/// Model: Grok 4.6 xHigh
pub(super) async fn setup_allow_create_format_version_3(
    wh: &TempDir,
) -> (SessionContext, CatalogRegistry) {
    setup_with_sql_settings(
        wh,
        true,
        repark_functions::cardinality::ReparkSqlSettings {
            allow_create_format_version_3: true,
            ..repark_functions::cardinality::ReparkSqlSettings::default()
        },
    )
    .await
}

pub(super) async fn live_deletion_vector_count(table: &iceberg::table::Table) -> usize {
    iceberg::live_deletion_vectors_by_data_file(table)
        .await
        .expect("count live deletion vectors")
        .len()
}

pub(super) async fn rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> usize {
    execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum()
}

/// A `SessionContext` + `CatalogRegistry`.
pub(super) async fn setup_strict_catalog(
    wh: &TempDir,
) -> (SessionContext, CatalogRegistry, String) {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let catalog: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .unwrap(),
    );
    let ctx = SessionContext::new_with_config(crate::extension::apply_spark_float_as_decimal(
        datafusion::prelude::SessionConfig::new(),
    ));
    repark_functions::decimal_spark::register_spark_decimal_planner(&ctx);
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "glue_like", catalog.clone())
        .await
        .unwrap();
    register_source(&ctx, "src", &[(1, "a"), (2, "b"), (3, "c")]);
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "glue_like".to_string(),
        catalog,
        LocationPolicy::RequireExplicitLocation,
    );
    (ctx, catalogs, warehouse)
}

/// Count `.parquet` files anywhere under `dir` — the CTAS data-placement value check.
pub(super) fn count_parquet_files(dir: &std::path::Path) -> usize {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    let mut count = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count += count_parquet_files(&path);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "parquet")
        {
            count += 1;
        }
    }
    count
}

/// The properties of `namespace` in the `glue_like` catalog (the WG-5 round-trip oracle).
pub(super) async fn namespace_props(
    catalogs: &CatalogRegistry,
    namespace: &str,
) -> HashMap<String, String> {
    catalogs["glue_like"]
        .get_namespace(&NamespaceIdent::new(namespace.to_string()))
        .await
        .unwrap()
        .properties()
        .clone()
}

/// Register a scalar UDF that errors when its int argument equals `2` (mid-stream failure pin).
pub(super) fn register_failing_scalar(ctx: &SessionContext) {
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
    };

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FailOnTwo {
        signature: Signature,
    }
    impl ScalarUDFImpl for FailOnTwo {
        fn name(&self) -> &'static str {
            "repark_fail_on_two"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
            Ok(DataType::Int32)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> datafusion::error::Result<ColumnarValue> {
            let ColumnarValue::Array(array) = &args.args[0] else {
                return Err(DataFusionError::Execution(
                    "repark_fail_on_two expected an array".into(),
                ));
            };
            let ints = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                DataFusionError::Execution("repark_fail_on_two expected Int32".into())
            })?;
            for index in 0..ints.len() {
                if ints.value(index) == 2 {
                    return Err(DataFusionError::Execution(
                        "injected CTAS source failure on row value 2".into(),
                    ));
                }
            }
            Ok(ColumnarValue::Array(Arc::new(ints.clone())))
        }
    }
    let udf = ScalarUDF::from(FailOnTwo {
        signature: Signature::exact(vec![DataType::Int32], Volatility::Volatile),
    });
    ctx.register_udf(udf);
}

/// Load `ice.sales.<table>` for I7 partition-evolution pins.
pub(super) async fn load_sales_table(
    catalogs: &CatalogRegistry,
    table: &str,
) -> iceberg::table::Table {
    catalog_handle(catalogs, "ice")
        .unwrap()
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            table.into(),
        ))
        .await
        .unwrap()
}

/// Execute a statement to completion (DML through DataFusion is lazy until collected).
pub(super) async fn run(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) {
    execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
}

/// Count live data-file scan tasks for rewrite file-count pins.
pub(super) async fn count_planned_data_files(catalog: &dyn Catalog, ident: &TableIdent) -> usize {
    use futures::TryStreamExt;
    let table = catalog.load_table(ident).await.expect("load");
    let scan = table.scan().build().expect("scan");
    let tasks: Vec<_> = scan
        .plan_files()
        .await
        .expect("plan_files")
        .try_collect()
        .await
        .expect("collect tasks");
    tasks.len()
}

/// Recursively count `*.parquet` files under `dir` (no-data-write proof).
pub(super) fn walk_parquet(dir: &std::path::Path, count: &mut usize) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk_parquet(&path, count);
            } else if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                *count += 1;
            }
        }
    }
}

/// Like [`setup`] but with `repark.sql.allowLocalFilesystemDDL=true` for COPY TO pins.
pub(super) async fn setup_allow_local_fs_ddl(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    setup_with_sql_settings(
        wh,
        true,
        repark_functions::cardinality::ReparkSqlSettings {
            allow_local_filesystem_ddl: true,
            ..repark_functions::cardinality::ReparkSqlSettings::default()
        },
    )
    .await
}

/// Register a two-column `(id, name)` in-memory source view for MERGE tests.
pub(super) fn register_source(ctx: &SessionContext, name: &str, rows: &[(i32, &str)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

pub(super) fn register_view_typed_source(ctx: &SessionContext, name: &str) {
    use datafusion::arrow::array::{BinaryViewArray, StringViewArray};
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8View, true),
        Field::new("payload", DataType::BinaryView, true),
        Field::new("id", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringViewArray::from(vec![Some("a"), None, Some("c")])),
            Arc::new(BinaryViewArray::from(vec![
                Some(b"x".as_slice()),
                Some(b"y".as_slice()),
                None,
            ])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

/// Register a string-pair source whose `a` values are not parseable as integers.
pub(super) fn register_unparsable_utf8_source(
    ctx: &SessionContext,
    name: &str,
    rows: &[(&str, &str)],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    ctx.register_batch(name, batch).unwrap();
}

/// Register an int-string source that yields multiple record batches.
pub(super) fn register_multi_batch_source(
    ctx: &SessionContext,
    name: &str,
    batches: &[&[(i32, &str)]],
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let record_batches = batches
        .iter()
        .map(|rows| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(
                        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let table = datafusion::datasource::MemTable::try_new(schema, vec![record_batches]).unwrap();
    ctx.register_table(name, Arc::new(table)).unwrap();
}

/// Read `id, name` back on the Arrow path, asserting Int32/Utf8 types, sorted by id.
pub(super) async fn read_back_typed(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, String)> {
    let batches = execute(ctx, catalogs, &format!("SELECT id, name FROM {table}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Int32,
            "id must read back as Int32 (value AND type)"
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Utf8,
            "name must read back as Utf8 (value AND type)"
        );
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), names.value(index).to_string()));
        }
    }
    rows.sort();
    rows
}

/// Read the whole table back as sorted `(id, name)` pairs — the MERGE result oracle.
pub(super) async fn table_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    table: &str,
) -> Vec<(i32, String)> {
    let batches = execute(
        ctx,
        catalogs,
        &format!("SELECT id, name FROM {table} ORDER BY id"),
    )
    .await
    .unwrap()
    .collect()
    .await
    .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), names.value(index).to_string()));
        }
    }
    rows
}

/// The live `(id, _file)` pairs from a core scan — proves copy-on-write file granularity.
pub(super) async fn id_file_pairs(catalogs: &CatalogRegistry, table: &str) -> Vec<(i32, String)> {
    use futures::TryStreamExt;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let scan = table.scan().select(["id", "_file"]).build().unwrap();
    let batches: Vec<RecordBatch> = scan.to_arrow().await.unwrap().try_collect().await.unwrap();
    let mut pairs = Vec::new();
    for batch in &batches {
        let ids = datafusion::arrow::compute::cast(batch.column(0), &DataType::Int32).unwrap();
        let ids = ids.as_any().downcast_ref::<Int32Array>().unwrap();
        let files = datafusion::arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let files = files.as_any().downcast_ref::<StringArray>().unwrap();
        for index in 0..batch.num_rows() {
            pairs.push((ids.value(index), files.value(index).to_string()));
        }
    }
    pairs
}

/// The number of live position/equality DELETE files in a table's current snapshot.
pub(super) async fn delete_file_count(catalogs: &CatalogRegistry, table: &str) -> usize {
    use iceberg::spec::ManifestContentType;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return 0;
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .unwrap();
    let mut count = 0;
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        count += manifest.entries().iter().filter(|e| e.is_alive()).count();
    }
    count
}

/// The live DATA-file paths in a table's current snapshot, read off the manifests.
pub(super) async fn live_data_file_paths(
    catalogs: &CatalogRegistry,
    table: &str,
) -> std::collections::HashSet<String> {
    use iceberg::spec::ManifestContentType;
    use std::collections::HashSet;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return HashSet::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .unwrap();
    let mut paths = HashSet::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        for entry in manifest.entries() {
            if entry.is_alive() {
                paths.insert(entry.data_file().file_path().to_string());
            }
        }
    }
    paths
}

/// Live DATA-file `partition_spec_id` values (I7 multi-spec write-after-evolution oracle).
pub(super) async fn live_data_file_spec_ids(
    catalogs: &CatalogRegistry,
    table: &str,
) -> std::collections::HashSet<i32> {
    use iceberg::spec::ManifestContentType;
    use std::collections::HashSet;
    let ident = TableIdent::new(NamespaceIdent::new("sales".to_string()), table.to_string());
    let table = catalogs["ice"].load_table(&ident).await.unwrap();
    let metadata = table.metadata();
    let Some(snapshot) = metadata.current_snapshot() else {
        return HashSet::new();
    };
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .unwrap();
    let mut specs = HashSet::new();
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Data {
            continue;
        }
        let manifest = manifest_file.load_manifest(table.file_io()).await.unwrap();
        for entry in manifest.entries() {
            if entry.is_alive() {
                specs.insert(entry.data_file().partition_spec_id());
            }
        }
    }
    specs
}

/// Sorted id multiset for time-travel integration pins (I1).
pub(super) async fn time_travel_id_multiset(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<i32> {
    use datafusion::arrow::array::{Array, AsArray};

    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("query {sql:?} failed: {error}"))
        .collect()
        .await
        .unwrap();
    let mut ids = Vec::new();
    for batch in batches {
        let col = batch
            .column(0)
            .as_primitive::<datafusion::arrow::datatypes::Int32Type>();
        for index in 0..col.len() {
            if col.is_valid(index) {
                ids.push(col.value(index));
            }
        }
    }
    ids.sort_unstable();
    ids
}

/// A plan-only `SessionContext` for pinning `logical_plan_has_unsafe_cast` on the shape it sees.
pub(super) fn cast_walk_ctx() -> SessionContext {
    let ctx = SessionContext::new();
    for rule in repark_functions::analyzer_rules() {
        ctx.add_analyzer_rule(rule);
    }
    register_source(&ctx, "src", &[(1, "a"), (2, "b"), (3, "c")]);
    register_unparsable_utf8_source(&ctx, "src2", &[("x", "y")]);
    ctx
}

/// Classify `source` exactly as `assert_empty_overwrite_types_assignment_compatible` does.
pub(super) async fn source_has_unsafe_cast(ctx: &SessionContext, source: &str) -> bool {
    let catalogs = CatalogRegistry::new();
    let df = spark_ast::execute_passthrough(
        ctx,
        &catalogs,
        &format!("SELECT * FROM ({source}) AS _repark_ow_types LIMIT 0"),
    )
    .await
    .expect("source must plan");
    logical_plan_has_unsafe_cast(df.logical_plan())
}

/// Execute a statement and drop its returned `DataFrame` without collecting it.
pub(super) async fn execute_without_collecting(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) {
    execute(ctx, catalogs, sql).await.unwrap();
}

#[allow(dead_code)]
pub(super) fn count_objects(root: &std::path::Path) -> usize {
    let mut pending = vec![root.to_path_buf()];
    let mut count = 0usize;
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else {
                count += 1;
            }
        }
    }
    count
}
