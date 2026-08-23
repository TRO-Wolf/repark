/// I2 / R-METADATA-TABLES: Spark `cat.ns.tbl.snapshots` → fork `$` provider; real table wins;
/// DML + AS OF composition refuse loud.
use super::super::*;
use super::common::*;

#[tokio::test]
#[allow(clippy::too_many_lines)] // multi-table matrix + real-wins + DML/AS OF guards
async fn metadata_tables_spark_dot_form_and_guards() {
    use datafusion::arrow::array::{Array, AsArray};

    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mt AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.mt SELECT 4 AS id, 'd' AS name",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.mt SELECT 5 AS id, 'e' AS name",
    )
    .await;

    // snapshots: ≥3 rows (3 append snapshots).
    let snap_batches = execute(
        &ctx,
        &catalogs,
        "SELECT snapshot_id, operation FROM ice.sales.mt.snapshots",
    )
    .await
    .expect("spark-style .snapshots")
    .collect()
    .await
    .unwrap();
    let snap_rows: usize = snap_batches.iter().map(RecordBatch::num_rows).sum();
    assert!(
        snap_rows >= 3,
        "expected ≥3 snapshots after CTAS+2 inserts, got {snap_rows}"
    );
    // Partial projection must return only requested columns (r25 T2 item 0 wrap).
    let snap_schema = snap_batches[0].schema();
    let snap_names: Vec<_> = snap_schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        snap_names,
        vec!["snapshot_id", "operation"],
        "partial SELECT must project (not full metadata schema)"
    );
    // Full-schema drift guard (restored, morning critic): SELECT * still pins the fork's
    // snapshots column set (fork inspect/snapshots.rs:49-73) so a fork-side schema change
    // goes red here, not in production.
    let snap_star = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.mt.snapshots")
        .await
        .expect("SELECT * .snapshots")
        .collect()
        .await
        .unwrap();
    let star_names: Vec<_> = snap_star[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        star_names,
        vec![
            "committed_at",
            "snapshot_id",
            "parent_id",
            "operation",
            "manifest_list",
            "summary"
        ],
        "snapshots schema names (fork inspect/snapshots.rs:49-73)"
    );

    // history — column names + at least one is_current_ancestor = true.
    let hist = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.mt.history")
        .await
        .expect("spark-style .history")
        .collect()
        .await
        .unwrap();
    assert!(!hist.is_empty(), "history must return batches");
    let hist_names: Vec<_> = hist[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(
        hist_names,
        vec![
            "made_current_at",
            "snapshot_id",
            "parent_id",
            "is_current_ancestor",
        ],
        "history schema names (fork inspect/history.rs:50-63)"
    );
    let ancestor_index = hist[0]
        .schema()
        .index_of("is_current_ancestor")
        .expect("is_current_ancestor column");
    let mut any_ancestor = false;
    for batch in &hist {
        let array = batch.column(ancestor_index);
        assert_eq!(
            array.data_type(),
            &datafusion::arrow::datatypes::DataType::Boolean,
            "is_current_ancestor type; all fields={:?}",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        );
        let col = array.as_boolean();
        for index in 0..col.len() {
            if col.is_valid(index) && col.value(index) {
                any_ancestor = true;
            }
        }
    }
    assert!(any_ancestor, "history must mark current-ancestor rows");

    // files.record_count sums to table row count (5).
    let files = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.mt.files")
        .await
        .expect("spark-style .files")
        .collect()
        .await
        .unwrap();
    assert!(!files.is_empty(), "files must return batches");
    let file_schema = files[0].schema();
    let record_index = file_schema.index_of("record_count").unwrap_or_else(|_| {
        panic!(
            "record_count missing; fields={:?}",
            file_schema
                .fields()
                .iter()
                .map(|f| format!("{}:{:?}", f.name(), f.data_type()))
                .collect::<Vec<_>>()
        )
    });
    let mut file_records: i64 = 0;
    for batch in &files {
        let array = batch.column(record_index);
        assert_eq!(
            array.data_type(),
            &datafusion::arrow::datatypes::DataType::Int64,
            "record_count type at index {record_index}"
        );
        let col = array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();
        for index in 0..col.len() {
            if col.is_valid(index) {
                file_records += col.value(index);
            }
        }
    }
    assert_eq!(file_records, 5, "files.record_count must sum to table rows");

    // Real table named `files` wins over metadata suffix interpretation.
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.files AS SELECT 42 AS x",
    )
    .await;
    let real = execute(&ctx, &catalogs, "SELECT x FROM ice.sales.files")
        .await
        .expect("real table ice.sales.files")
        .collect()
        .await
        .unwrap();
    assert_eq!(real[0].num_rows(), 1);
    // CTAS-inferred integer literals are Int64 on the Iceberg/Arrow path (same as time-travel pins).
    let x_col = real[0]
        .column(0)
        .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
    assert_eq!(x_col.value(0), 42);
    // Must NOT be the files metadata schema (content/file_path/…).
    assert_eq!(real[0].schema().field(0).name(), "x");

    // DML targeting metadata table is loud.
    let dml_err = execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.mt.snapshots SELECT 1",
    )
    .await
    .expect_err("DML on metadata table");
    let dml_msg = dml_err.to_string();
    assert!(
        dml_msg.contains("read-only") || dml_msg.contains("metadata table"),
        "DML error must name metadata read-only, got: {dml_msg}"
    );

    // AS OF composition is out of scope v1.
    let asof_err = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.mt.snapshots VERSION AS OF 1",
    )
    .await
    .expect_err("AS OF + metadata");
    let asof_msg = asof_err.to_string();
    assert!(
        asof_msg.contains("not supported") || asof_msg.contains("time travel"),
        "composition error must disclose out-of-scope, got: {asof_msg}"
    );

    // C1-Q-002: rewrite string must land on fork `$` form (mutation pin).
    let rewritten = metadata_tables::prepare_metadata_table_sql(
        &catalogs,
        "SELECT * FROM ice.sales.mt.snapshots",
    )
    .await
    .expect("prepare rewrite")
    .expect("must rewrite spark-style path");
    assert!(
        rewritten.contains("mt$snapshots"),
        "rewrite must produce fork $ form, got: {rewritten}"
    );
    assert!(
        !rewritten.contains("mt.snapshots"),
        "dotted meta suffix must not survive rewrite: {rewritten}"
    );

    // C1-L-002: parenthesized AS OF still refused.
    let paren_asof = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM (ice.sales.mt.snapshots) VERSION AS OF 1",
    )
    .await
    .expect_err("paren AS OF + metadata");
    let paren_msg = paren_asof.to_string();
    assert!(
        paren_msg.contains("not supported") || paren_msg.contains("time travel"),
        "paren composition must refuse loud, got: {paren_msg}"
    );

    // C1-L-003: metadata of a real table literally named `files`.
    let files_meta = execute(&ctx, &catalogs, "SELECT * FROM ice.sales.files.snapshots")
        .await
        .expect("metadata of real table named files")
        .collect()
        .await
        .expect("collect files.snapshots");
    assert!(
        !files_meta.is_empty(),
        "files.snapshots must resolve via files$snapshots"
    );

    // C1-Q-003: UPDATE targeting metadata is loud; INSERT into real `files` is not.
    let update_err = execute(
        &ctx,
        &catalogs,
        "UPDATE ice.sales.mt.history SET snapshot_id = 1",
    )
    .await
    .expect_err("UPDATE metadata");
    let update_msg = update_err.to_string();
    assert!(
        update_msg.contains("read-only") || update_msg.contains("metadata table"),
        "UPDATE error must name metadata read-only, got: {update_msg}"
    );
    let insert_real = execute(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.files SELECT 99 AS x",
    )
    .await;
    assert!(
        insert_real.is_ok(),
        "INSERT into real table named files must not be blocked as metadata: {insert_real:?}"
    );

    // C2-Q-001: JOIN metadata relation rewrites and scans.
    let join_batches = execute(
        &ctx,
        &catalogs,
        "SELECT f.record_count FROM ice.sales.mt JOIN ice.sales.mt.files f ON true",
    )
    .await
    .expect("JOIN metadata files")
    .collect()
    .await
    .expect("collect JOIN files");
    let join_rows: usize = join_batches.iter().map(RecordBatch::num_rows).sum();
    assert!(join_rows >= 1, "JOIN to .files must return rows");

    // C2-Q-002: TRUNCATE + CREATE OR REPLACE on metadata refuse loud.
    let trunc_err = execute(&ctx, &catalogs, "TRUNCATE TABLE ice.sales.mt.files")
        .await
        .expect_err("TRUNCATE metadata");
    let trunc_msg = trunc_err.to_string();
    assert!(
        trunc_msg.contains("read-only") || trunc_msg.contains("metadata table"),
        "TRUNCATE error must name metadata read-only, got: {trunc_msg}"
    );
    let cor_err = execute(
        &ctx,
        &catalogs,
        "CREATE OR REPLACE TABLE ice.sales.mt.snapshots AS SELECT 1 AS id",
    )
    .await
    .expect_err("CREATE OR REPLACE metadata");
    let cor_msg = cor_err.to_string();
    assert!(
        cor_msg.contains("read-only") || cor_msg.contains("metadata table"),
        "CREATE OR REPLACE error must name metadata read-only, got: {cor_msg}"
    );

    // H-1d: registry §2.1 row MT-2 enumerates TEN statement forms as refusing with a read-only
    // error. Four of them are pinned above (INSERT / UPDATE / TRUNCATE / CREATE OR REPLACE); the
    // remaining six were prose until this loop existed. A row may not assert more than its pin
    // proves, so every form the row names is exercised here — all ten reach the same
    // `is_write_target_context` guard, and the refusal is what keeps a write to a metadata table
    // from falling through to the planner.
    for sql in [
        "DELETE FROM ice.sales.mt.snapshots WHERE snapshot_id = 1",
        "MERGE INTO ice.sales.mt.snapshots t USING ice.sales.mt s ON true \
         WHEN MATCHED THEN UPDATE SET *",
        "CREATE TABLE ice.sales.mt.snapshots AS SELECT 1 AS id",
        "CREATE VIEW ice.sales.mt.snapshots AS SELECT 1 AS id",
        "DROP TABLE ice.sales.mt.snapshots",
        "ALTER TABLE ice.sales.mt.snapshots ADD COLUMN extra INT",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a write targeting a metadata table must refuse, never fall through")
            .to_string();
        assert!(
            error.contains("metadata table") && error.contains("read-only"),
            "the refusal must name metadata read-only, not merely error ({sql}): {error}"
        );
    }

    // C2-L-001: multi-span rewrite produces two `$` forms.
    let multi = metadata_tables::prepare_metadata_table_sql(
        &catalogs,
        "SELECT * FROM ice.sales.mt.snapshots JOIN ice.sales.mt.files ON true",
    )
    .await
    .expect("prepare multi")
    .expect("must rewrite both spans");
    assert!(
        multi.contains("mt$snapshots") && multi.contains("mt$files"),
        "multi-span rewrite must produce both $ forms, got: {multi}"
    );

    // C3-Q-001: TIMESTAMP / SYSTEM_* AS OF composition refuse loud.
    for sql in [
        "SELECT * FROM ice.sales.mt.snapshots TIMESTAMP AS OF '2099-01-01 00:00:00'",
        "SELECT * FROM ice.sales.mt.files FOR SYSTEM_VERSION AS OF 1",
        "SELECT * FROM ice.sales.mt.history FOR SYSTEM_TIME AS OF '2099-01-01 00:00:00'",
    ] {
        let err = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("TIMESTAMP/SYSTEM AS OF + metadata");
        let msg = err.to_string();
        assert!(
            msg.contains("not supported") || msg.contains("time travel"),
            "AS OF form must refuse loud ({sql}): {msg}"
        );
    }

    // C3-L-002: metadata join + base table VERSION AS OF (meta first, then TT).
    // Snapshot id 1 is almost certainly invalid — pin only that the error is NOT the
    // metadata-composition refuse (wrong guard would fire before TT resolves the base).
    let mixed_err = execute(
        &ctx,
        &catalogs,
        "SELECT * FROM ice.sales.mt.files f JOIN ice.sales.mt VERSION AS OF 1 t ON true",
    )
    .await
    .expect_err("mixed query should fail on snapshot id or succeed; not composition");
    // If it somehow succeeds in future fixtures, the expect_err will force a revisit.
    let mixed_msg = mixed_err.to_string();
    assert!(
        !mixed_msg.contains("composed with Iceberg metadata"),
        "mixed base AS OF + metadata join must not hit metadata-composition refuse: {mixed_msg}"
    );

    // C7-Q-001: DESCRIBE rewrites to `$` form (read path).
    let describe_sql =
        metadata_tables::prepare_metadata_table_sql(&catalogs, "DESCRIBE TABLE ice.sales.mt.files")
            .await
            .expect("prepare DESCRIBE")
            .expect("DESCRIBE meta must rewrite");
    assert!(
        describe_sql.contains("mt$files"),
        "DESCRIBE must rewrite to $ form, got: {describe_sql}"
    );
}

/// ADR-0006 (campaign decision D2, unit H-1c) at the **Spark door**: the fork's synthesized
/// `$`-metadata names do not enumerate, and the Spark dotted spelling that rewrites onto them
/// still works. The decision is made once at the catalog layer
/// (`repark_iceberg::catalog::MetadataProjectionSchemaProvider::table_names`), never in a door
/// parser — this row is what proves the single decision reaches THIS door.
///
/// Risk pinned: enumeration and resolution are different surfaces, and a filter placed wrong
/// breaks the second while satisfying the first. Both are asserted here, plus the twin path
/// (`SHOW TABLES` and `information_schema.tables`) that a fix touching only one of them would
/// leave inconsistent.
///
/// Mutation: drop the `.filter(…)` in `MetadataProjectionSchemaProvider::table_names` → the two
/// emptiness assertions red.
///
/// pins: rp-1-fork-repin/C-005
#[tokio::test]
async fn metadata_tables_are_hidden_from_enumeration_but_stay_queryable_through_the_spark_door() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    // `information_schema` is off by default (repark-core's builder key is the product way to
    // turn it on); the door tests run on a raw context, so arm it here.
    ctx.sql("SET datafusion.catalog.information_schema = true")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.hidden AS SELECT * FROM src",
    )
    .await;

    // 1. information_schema.tables — the real table only.
    let listed = execute(
        &ctx,
        &catalogs,
        "SELECT table_name FROM information_schema.tables \
         WHERE table_catalog = 'ice' AND table_schema = 'sales'",
    )
    .await
    .expect("information_schema must plan through the Spark door")
    .collect()
    .await
    .unwrap();
    let mut names: Vec<String> = Vec::new();
    for batch in &listed {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("table_name is Utf8");
        for row in 0..batch.num_rows() {
            names.push(column.value(row).to_string());
        }
    }
    names.sort();
    assert_eq!(
        names,
        vec!["hidden".to_string()],
        "the Spark door must enumerate the catalog's tables, not the fork's synthesized names"
    );

    // 2. The twin path.
    let shown = execute(&ctx, &catalogs, "SHOW TABLES")
        .await
        .expect("SHOW TABLES must plan through the Spark door")
        .collect()
        .await
        .unwrap();
    let mut dollar_names: Vec<String> = Vec::new();
    for batch in &shown {
        let column = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .expect("table_name is Utf8");
        for row in 0..batch.num_rows() {
            if column.value(row).contains('$') {
                dollar_names.push(column.value(row).to_string());
            }
        }
    }
    assert!(
        dollar_names.is_empty(),
        "SHOW TABLES must not list metadata tables through this door either: {dollar_names:?}"
    );

    // 3. Hidden, not removed — through the door's OWN spelling (`t.snapshots`, which the door
    //    rewrites onto the hidden `t$snapshots`) and through the `$` form directly.
    let dotted = execute(
        &ctx,
        &catalogs,
        "SELECT snapshot_id FROM ice.sales.hidden.snapshots",
    )
    .await
    .expect("the Spark dotted spelling must still resolve")
    .collect()
    .await
    .unwrap();
    assert_eq!(
        dotted.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "one CTAS snapshot must be visible through the dotted spelling"
    );
    let dollar = execute(
        &ctx,
        &catalogs,
        "SELECT snapshot_id FROM ice.sales.\"hidden$snapshots\"",
    )
    .await
    .expect("the `$` spelling must still resolve")
    .collect()
    .await
    .unwrap();
    assert_eq!(
        dollar.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "the hidden name must stay addressable directly"
    );
}

/// r25 T2 item 0: metadata-table projection honor — empty (`count`), partial, full `SELECT *`
/// parameterized across ALL supported metadata table names (`LocalFS` memory catalog only).
///
/// Root cause: fork `IcebergMetadataTableProvider::scan` ignores projection; wrap
/// at `SchemaProvider` registration with `ProjectionExec` (never collect-then-project).
#[tokio::test]
#[allow(clippy::too_many_lines)] // one flat battery over the full MetadataTableType set
async fn metadata_table_projection_honor_all_types() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.proj AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.proj SELECT 4 AS id, 'd' AS name",
    )
    .await;

    // Every fork MetadataTableType::as_str (same static set as metadata_tables::METADATA_TABLE_NAMES).
    let all_meta = [
        "snapshots",
        "manifests",
        "files",
        "data_files",
        "delete_files",
        "entries",
        "all_files",
        "all_data_files",
        "all_delete_files",
        "all_entries",
        "history",
        "refs",
        "metadata_log_entries",
        "partitions",
        "all_manifests",
    ];
    for suffix in all_meta {
        let table_path = format!("ice.sales.proj.{suffix}");

        // Full SELECT * — plan schema non-empty + collect must not Internal-error.
        let star_df = execute(&ctx, &catalogs, &format!("SELECT * FROM {table_path}"))
            .await
            .unwrap_or_else(|err| panic!("SELECT * FROM {table_path}: {err}"));
        let full_width = star_df.schema().fields().len();
        assert!(
            full_width > 0,
            "{suffix}: SELECT * logical schema must be non-empty"
        );
        let first_col = star_df.schema().field(0).name().clone();
        let star_batches = star_df
            .collect()
            .await
            .unwrap_or_else(|err| panic!("collect * {table_path}: {err}"));
        let star_rows: usize = star_batches.iter().map(RecordBatch::num_rows).sum();

        // Empty projection / count — the user-reported failure class (logical 0 vs physical N).
        let count_df = execute(
            &ctx,
            &catalogs,
            &format!("SELECT count(*) FROM {table_path}"),
        )
        .await
        .unwrap_or_else(|err| panic!("count(*) {table_path}: {err}"));
        let count_batches = count_df
            .collect()
            .await
            .unwrap_or_else(|err| panic!("collect count {table_path}: {err}"));
        assert!(
            !count_batches.is_empty(),
            "{suffix}: count(*) must produce a batch"
        );
        assert_eq!(
            count_batches[0].num_columns(),
            1,
            "{suffix}: count(*) returns one aggregate column"
        );
        // Value pin (morning critic): count(*) must equal the SELECT * row total — a
        // zero-column projection that lost `num_rows` would return 0 and stay green on
        // shape alone. snapshots/history are additionally pinned exact (CTAS + INSERT = 2).
        let counted = count_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("{suffix}: count(*) column must be Int64"))
            .value(0);
        assert_eq!(
            counted,
            i64::try_from(star_rows).expect("row count fits i64"),
            "{suffix}: count(*) value must equal SELECT * row total"
        );
        if matches!(suffix, "snapshots" | "history") {
            assert_eq!(
                counted, 2,
                "{suffix}: CTAS + INSERT must yield exactly 2 {suffix} rows"
            );
        }

        // Partial projection — first column only (logical + physical schema width 1).
        let partial_df = execute(
            &ctx,
            &catalogs,
            &format!("SELECT \"{first_col}\" FROM {table_path}"),
        )
        .await
        .unwrap_or_else(|err| panic!("partial SELECT {first_col} FROM {table_path}: {err}"));
        assert_eq!(
            partial_df.schema().fields().len(),
            1,
            "{suffix}: partial projection must be 1 field, got {:?}",
            partial_df
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            partial_df.schema().field(0).name(),
            &first_col,
            "{suffix}: projected column name"
        );
        let partial_batches = partial_df
            .collect()
            .await
            .unwrap_or_else(|err| panic!("collect partial {table_path}: {err}"));
        // When rows exist, collected batches must also be single-column (physical path).
        if let Some(batch) = partial_batches.first() {
            assert_eq!(
                batch.num_columns(),
                1,
                "{suffix}: collected partial must be 1 column"
            );
        }
    }
}

/// Glue/HMS `validate_namespace`: a namespace that is not exactly one level is
/// `ErrorKind::DataInvalid`, not `NamespaceNotFound`. MW-4 live Glue failed here:
/// `cat.ns.tbl.snapshots` probes ident `{ns:[ns, tbl], table: snapshots}`.
fn glue_hierarchical_namespace_error(namespace: &NamespaceIdent) -> iceberg::Error {
    iceberg::Error::new(
        iceberg::ErrorKind::DataInvalid,
        format!("Invalid database name: {namespace:?}, hierarchical namespaces are not supported"),
    )
}

/// How `table_exists` fails on a two-level namespace. `Glue` is the live AWS shape.
#[derive(Debug, Clone, Copy)]
enum HierarchicalExistsProbe {
    Glue,
    Unexpected,
    AlwaysDataInvalid,
}

/// Memory catalog plus Glue/HMS namespace-shape rules on `table_exists` only.
#[derive(Debug)]
struct GlueNamespaceShapeCatalog {
    inner: Arc<dyn Catalog>,
    probe: HierarchicalExistsProbe,
}

#[async_trait::async_trait]
impl Catalog for GlueNamespaceShapeCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        let level_count = table.namespace().as_ref().len();
        match self.probe {
            HierarchicalExistsProbe::Glue if level_count != 1 => {
                return Err(glue_hierarchical_namespace_error(table.namespace()));
            }
            HierarchicalExistsProbe::Unexpected if level_count != 1 => {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    "injected hierarchical Unexpected",
                ));
            }
            HierarchicalExistsProbe::AlwaysDataInvalid => {
                return Err(iceberg::Error::new(
                    iceberg::ErrorKind::DataInvalid,
                    "Invalid database, provided namespace is empty.",
                ));
            }
            _ => {}
        }
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(
        &self,
        commit: iceberg::TableCommit,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.update_table(commit).await
    }
}

fn glue_shaped_registry(
    inner: Arc<dyn Catalog>,
    probe: HierarchicalExistsProbe,
) -> CatalogRegistry {
    let wrapped: Arc<dyn Catalog> = Arc::new(GlueNamespaceShapeCatalog { inner, probe });
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "glue_catalog".to_string(),
        wrapped,
        LocationPolicy::RequireExplicitLocation,
    );
    catalogs
}

/// MW-4 live Glue: `SELECT … FROM glue_catalog.ns.tbl.snapshots` must rewrite to `$`
/// even though Glue `table_exists` on the 4-part path is `DataInvalid`, not not-found.
#[tokio::test]
async fn glue_shaped_catalog_rewrites_four_part_snapshots_and_files() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mt AS SELECT * FROM src",
    )
    .await;
    let inner = catalogs.get("ice").expect("ice catalog").clone();
    let glue_catalogs = glue_shaped_registry(inner, HierarchicalExistsProbe::Glue);

    let snapshots = metadata_tables::prepare_metadata_table_sql(
        &glue_catalogs,
        "SELECT snapshot_id FROM glue_catalog.sales.mt.snapshots ORDER BY committed_at",
    )
    .await
    .expect("Glue-shaped snapshots probe must not surface DataInvalid")
    .expect("must rewrite to $ form");
    assert!(
        snapshots.contains("mt$snapshots"),
        "Glue-shaped .snapshots must rewrite, got: {snapshots}"
    );
    assert!(
        !snapshots.contains(".snapshots"),
        "rewritten SQL must not keep the dotted suffix: {snapshots}"
    );

    let files = metadata_tables::prepare_metadata_table_sql(
        &glue_catalogs,
        "SELECT content FROM glue_catalog.sales.mt.files",
    )
    .await
    .expect("Glue-shaped files probe must not surface DataInvalid")
    .expect("must rewrite files");
    assert!(
        files.contains("mt$files"),
        "Glue-shaped .files must rewrite, got: {files}"
    );
}

#[tokio::test]
async fn glue_shaped_unexpected_on_hierarchical_namespace_stays_fatal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mt AS SELECT * FROM src",
    )
    .await;
    let inner = catalogs.get("ice").expect("ice catalog").clone();
    let glue_catalogs = glue_shaped_registry(inner, HierarchicalExistsProbe::Unexpected);

    let error = metadata_tables::prepare_metadata_table_sql(
        &glue_catalogs,
        "SELECT snapshot_id FROM glue_catalog.sales.mt.snapshots",
    )
    .await
    .expect_err("Unexpected on a hierarchical probe must stay fatal");
    let message = error.to_string();
    assert!(
        message.contains("Unexpected") || message.contains("injected hierarchical"),
        "must not swallow Unexpected as a rewrite, got: {message}"
    );
}

#[tokio::test]
async fn glue_shaped_data_invalid_on_single_level_namespace_stays_fatal() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.mt AS SELECT * FROM src",
    )
    .await;
    let inner = catalogs.get("ice").expect("ice catalog").clone();
    let glue_catalogs = glue_shaped_registry(inner, HierarchicalExistsProbe::AlwaysDataInvalid);

    let error = metadata_tables::prepare_metadata_table_sql(
        &glue_catalogs,
        "SELECT snapshot_id FROM glue_catalog.sales.mt.snapshots",
    )
    .await
    .expect_err("single-level DataInvalid must stay fatal");
    let message = error.to_string();
    assert!(
        message.contains("DataInvalid") || message.contains("Invalid database"),
        "must not treat every DataInvalid as absent, got: {message}"
    );
}
