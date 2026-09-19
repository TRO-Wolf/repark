//! V3-2 — ANSI CREATE/CTAS `format_version = 3` behind the session opt-in.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use crate::execute;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    /// Model: Grok 4.6 xHigh
    async fn sql(
        &self,
        sql: &str,
    ) -> datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>> {
        let read_only = HashSet::new();
        let frame = execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    /// Model: Grok 4.6 xHigh
    async fn ok(&self, sql: &str) {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
    }

    /// Model: Grok 4.6 xHigh
    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    /// Model: Grok 4.6 xHigh
    async fn table(&self, namespace: &str, table: &str) -> iceberg::table::Table {
        self.catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("`{namespace}.{table}` must load: {err}"))
    }

    /// Model: Grok 4.6 xHigh
    async fn table_exists(&self, namespace: &str, table: &str) -> bool {
        self.catalog
            .table_exists(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .expect("table_exists")
    }
}

/// Model: Grok 4.6 xHigh
async fn door_with_config(config: SessionConfig) -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();

    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let ctx = SessionContext::new_with_config(config);
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
        .await
        .expect("register catalog");

    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        Arc::clone(&catalog),
        LocationPolicy::TempFallbackAllowed {
            root: warehouse_dir.path().to_path_buf(),
        },
    );
    catalogs.note_local_warehouse_root(&warehouse);

    Door {
        ctx,
        catalogs,
        catalog,
        warehouse,
        _warehouse_dir: warehouse_dir,
    }
}

/// Model: Grok 4.6 xHigh
async fn door_with_schema() -> Door {
    let door = door_with_config(SessionConfig::new().with_information_schema(true)).await;
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

/// Stand-in for `SparkExtension`'s `ReparkSqlConfig` (SEC-02 pattern — no product functions edge).
#[derive(Debug, Clone, Default)]
struct TestAllowCreateV3Config {
    allow: bool,
}

impl ConfigExtension for TestAllowCreateV3Config {
    const PREFIX: &'static str = "repark.sql";
}

impl ExtensionOptions for TestAllowCreateV3Config {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }
    fn set(&mut self, key: &str, value: &str) -> datafusion::error::Result<()> {
        if key == "allow_create_format_version_3" {
            self.allow = value.eq_ignore_ascii_case("true");
        }
        Ok(())
    }
    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: "repark.sql.allow_create_format_version_3".to_string(),
            value: Some(self.allow.to_string()),
            description: "test stand-in for V3-2 CREATE opt-in",
        }]
    }
}

/// Model: Grok 4.6 xHigh
async fn door_with_v3_opt_in() -> Door {
    let mut config = SessionConfig::new().with_information_schema(true);
    config
        .options_mut()
        .extensions
        .insert(TestAllowCreateV3Config { allow: true });
    let door = door_with_config(config).await;
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

async fn door_with_session_v3_opt_in() -> Door {
    let config = repark_functions::cardinality::with_repark_sql_config(
        SessionConfig::new().with_information_schema(true),
        repark_functions::cardinality::ReparkSqlSettings {
            allow_create_format_version_3: true,
            ..repark_functions::cardinality::ReparkSqlSettings::default()
        },
    );
    let door = door_with_config(config).await;
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

/// pins: v3-2-create-v3-opt-in/C-004
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn format_version_three_without_opt_in_refuses() {
    let door = door_with_schema().await;
    let err = door
        .err("CREATE TABLE ice.sales.v3 WITH (format_version = 3) AS SELECT 1 AS id")
        .await;
    assert!(
        err.contains("repark.sql.allowCreateFormatVersion3") && err.contains("format_version"),
        "opt-in refuse must name conf and property: {err}"
    );
    let _: &str = "pins: v3-9-mor-predicate-dml-dv/C-006";
    assert!(
        !err.contains("merge-on-read"),
        "V3-9 serves merge-on-read row-level writes on v3: {err}"
    );
    assert!(!door.table_exists("sales", "v3").await, "nothing created");

    let err = door
        .err("CREATE TABLE ice.sales.v3c (id BIGINT) WITH (format_version = 3)")
        .await;
    assert!(
        err.contains("repark.sql.allowCreateFormatVersion3"),
        "column-def must refuse too: {err}"
    );
}

/// pins: v3-2-create-v3-opt-in/C-002, C-006, C-013
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn format_version_three_opt_in_creates_v3() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.v3 WITH (format_version = 3) AS SELECT 1 AS id")
        .await;
    let table = door.table("sales", "v3").await;
    assert_eq!(
        table.metadata().format_version() as u8,
        3,
        "opt-in CTAS must create format v3"
    );

    door.ok("CREATE TABLE ice.sales.v3c (id BIGINT) WITH (format_version = 3)")
        .await;
    let column_def = door.table("sales", "v3c").await;
    assert_eq!(
        column_def.metadata().format_version() as u8,
        3,
        "opt-in column-def CREATE must create format v3"
    );

    door.ok("CREATE TABLE ice.sales.still_v2 AS SELECT 1 AS id")
        .await;
    let still = door.table("sales", "still_v2").await;
    assert_eq!(
        still.metadata().format_version() as u8,
        2,
        "opt-in must not change the unspecified default"
    );
}

/// pins: v3-2-create-v3-opt-in/C-006
/// Model: Grok 4.6 xHigh
#[tokio::test]
async fn or_replace_applies_requested_v3() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.up (id BIGINT)").await;
    door.ok("CREATE OR REPLACE TABLE ice.sales.up (id BIGINT) WITH (format_version = 3)")
        .await;
    let upgraded = door.table("sales", "up").await;
    assert_eq!(upgraded.metadata().format_version() as u8, 3);
    door.ok("CREATE OR REPLACE TABLE ice.sales.up (id BIGINT)")
        .await;
    let kept = door.table("sales", "up").await;
    assert_eq!(
        kept.metadata().format_version() as u8,
        3,
        "unspecified OR REPLACE must not force v2 onto an existing v3 table"
    );
}

/// pins: v3-6-v3-types/C-003
/// Model: GLM (glm-5.3-flash)
#[tokio::test]
async fn opt_in_v3_create_stores_timestamp_ns() {
    use iceberg::spec::{PrimitiveType, Type};

    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.tsns (id INT, ts timestamp_ns) WITH (format_version = 3)")
        .await;
    let table = door.table("sales", "tsns").await;
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestampNs)
    );
}

/// pins: v3-6-v3-types/C-003
/// Model: GLM (glm-5.3-flash)
#[tokio::test]
async fn opt_in_v3_create_stores_timestamptz_ns() {
    use iceberg::spec::{PrimitiveType, Type};

    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.tstzns (id INT, ts timestamptz_ns) WITH (format_version = 3)")
        .await;
    let table = door.table("sales", "tstzns").await;
    assert_eq!(
        table.metadata().current_schema().as_struct().fields()[1]
            .field_type
            .as_ref(),
        &Type::Primitive(PrimitiveType::TimestamptzNs)
    );
}

/// pins: v3-6-v3-types/C-003
/// Model: GLM (glm-5.3-flash)
#[tokio::test]
async fn opt_in_v3_timestamp_ns_select_round_trips_ns_values() {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch, TimestampNanosecondArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.tsnsrt (id INT, ts timestamp_ns) WITH (format_version = 3)")
        .await;
    let ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "tsnsrt".to_string(),
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

#[tokio::test]
async fn alter_set_properties_upgrades_v2_to_v3_with_the_opt_in() {
    let door = door_with_session_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.up AS SELECT 1 AS id").await;
    let before = door.table("sales", "up").await;
    assert_eq!(before.metadata().format_version() as u8, 2);
    let snapshots_before = before.metadata().snapshots().count();

    door.ok("ALTER TABLE ice.sales.up SET PROPERTIES (format_version = 3)")
        .await;

    let after = door.table("sales", "up").await;
    assert_eq!(after.metadata().format_version() as u8, 3);
    assert_eq!(after.metadata().next_row_id(), 0);
    assert_eq!(after.metadata().snapshots().count(), snapshots_before);
    assert!(!after.metadata().properties().contains_key("format-version"));
    let lineage = door
        .sql("SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.up")
        .await
        .expect("v3 lineage columns resolve after the upgrade");
    assert_eq!(lineage[0].num_rows(), 1);
    for column in [1usize, 2] {
        assert_eq!(
            lineage[0].column(column).null_count(),
            1,
            "a pre-upgrade row carries no lineage until a later v3 commit assigns it"
        );
    }

    let log_before = after.metadata().metadata_log().len();
    door.ok("ALTER TABLE ice.sales.up SET PROPERTIES (format_version = 3)")
        .await;
    let repeated = door.table("sales", "up").await;
    assert_eq!(repeated.metadata().format_version() as u8, 3);
    assert_eq!(
        repeated.metadata().metadata_log().len(),
        log_before,
        "a same-version request writes no metadata file, as Spark writes none"
    );
}

#[tokio::test]
async fn alter_set_properties_extra_properties_map_spelling_steers_to_the_curated_key() {
    let door = door_with_session_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.xp AS SELECT 1 AS id").await;
    let err = door
        .err(
            "ALTER TABLE ice.sales.xp SET PROPERTIES (extra_properties = \
             MAP(ARRAY['format-version'], ARRAY['3']))",
        )
        .await;
    assert!(
        err.contains("format-version") && err.contains("reserved"),
        "the raw hatch keeps steering to the curated key: {err}"
    );
    assert_eq!(
        door.table("sales", "xp").await.metadata().format_version() as u8,
        2
    );
}

#[tokio::test]
async fn alter_set_properties_upgrade_refuses_without_the_opt_in() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.up AS SELECT 1 AS id").await;
    let err = door
        .err("ALTER TABLE ice.sales.up SET PROPERTIES (format_version = 3)")
        .await;
    assert!(
        err.contains("repark.sql.allowCreateFormatVersion3") && err.contains("format-version"),
        "the refusal must name the conf and the key: {err}"
    );
    assert_eq!(
        door.table("sales", "up").await.metadata().format_version() as u8,
        2
    );
}

#[tokio::test]
async fn alter_set_properties_downgrade_and_unsupported_versions_refuse() {
    let door = door_with_session_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.dg AS SELECT 1 AS id").await;
    door.ok("ALTER TABLE ice.sales.dg SET PROPERTIES (format_version = 3)")
        .await;
    let down = door
        .err("ALTER TABLE ice.sales.dg SET PROPERTIES (format_version = 2)")
        .await;
    assert!(
        down.contains("format-version") && down.contains("v3") && down.contains("v2"),
        "downgrade must name the key and both versions: {down}"
    );

    door.ok("CREATE TABLE ice.sales.bad AS SELECT 1 AS id")
        .await;
    for (value, needle) in [
        ("1", "v1"),
        ("'-1'", "v-1"),
        ("4", "v1 through v3"),
        ("'x'", "not an Iceberg"),
        ("'3.0'", "not an Iceberg"),
    ] {
        let err = door
            .err(&format!(
                "ALTER TABLE ice.sales.bad SET PROPERTIES (format_version = {value})"
            ))
            .await;
        assert!(
            err.contains("format-version") && err.contains(needle),
            "`{value}` must refuse naming `{needle}`: {err}"
        );
    }
    assert_eq!(
        door.table("sales", "bad").await.metadata().format_version() as u8,
        2
    );
}

const MOR_V2: &str = "format_version = 2, extra_properties = MAP(\
    ARRAY['write.delete.mode', 'write.merge.mode'], \
    ARRAY['merge-on-read', 'merge-on-read'])";

async fn live_delete_file_kinds(door: &Door, table: &str) -> Vec<String> {
    use iceberg::spec::ManifestContentType;
    let loaded = door.table("sales", table).await;
    let metadata = loaded.metadata();
    let mut kinds = Vec::new();
    let Some(snapshot) = metadata.current_snapshot() else {
        return kinds;
    };
    let manifest_list = snapshot
        .load_manifest_list(loaded.file_io(), metadata)
        .await
        .expect("manifest list");
    for manifest_file in manifest_list.entries() {
        if manifest_file.content != ManifestContentType::Deletes {
            continue;
        }
        let manifest = manifest_file
            .load_manifest(loaded.file_io())
            .await
            .expect("manifest");
        for entry in manifest.entries() {
            if entry.is_alive() {
                kinds.push(format!("{:?}", entry.data_file().file_format()));
            }
        }
    }
    kinds.sort();
    kinds
}

async fn surviving_ids(door: &Door, table: &str) -> Vec<i32> {
    use datafusion::arrow::array::Int32Array;
    let batches = door
        .sql(&format!("SELECT id FROM ice.sales.{table} ORDER BY id"))
        .await
        .unwrap_or_else(|err| panic!("select on {table}: {err}"));
    let mut ids = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id Int32");
        for index in 0..batch.num_rows() {
            ids.push(column.value(index));
        }
    }
    ids
}

#[tokio::test]
async fn upgraded_v3_merge_delete_merges_a_legacy_parquet_position_delete_into_the_dv() {
    let door = door_with_session_v3_opt_in().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.legacy (id INT, name VARCHAR) WITH ({MOR_V2})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.legacy VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    door.ok(
        "MERGE INTO ice.sales.legacy AS t USING (SELECT 2 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    )
    .await;
    assert_eq!(
        live_delete_file_kinds(&door, "legacy").await,
        vec!["Parquet".to_string()],
        "the v2 arm leaves a parquet position delete"
    );

    door.ok("ALTER TABLE ice.sales.legacy SET PROPERTIES (format_version = 3)")
        .await;
    door.ok(
        "MERGE INTO ice.sales.legacy AS t USING (SELECT 3 AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE",
    )
    .await;

    assert_eq!(
        surviving_ids(&door, "legacy").await,
        vec![1],
        "both the legacy position and the new one stay deleted"
    );
    assert_eq!(
        live_delete_file_kinds(&door, "legacy").await,
        vec!["Puffin".to_string()],
        "the superseded parquet delete leaves in the same RowDelta the DV arrives in"
    );
    assert_eq!(
        door.table("sales", "legacy").await.metadata().next_row_id(),
        3
    );
}

const MOR_V2_PARTITION_GRANULARITY: &str = "format_version = 2, extra_properties = MAP(\
    ARRAY['write.delete.mode', 'write.merge.mode', 'write.delete.granularity'], \
    ARRAY['merge-on-read', 'merge-on-read', 'partition'])";

async fn seed_upgraded_legacy(door: &Door, table: &str, with_clause: &str, seeds: &[&str]) {
    door.ok(&format!(
        "CREATE TABLE ice.sales.{table} (id INT, name VARCHAR) WITH ({with_clause})"
    ))
    .await;
    for values in seeds {
        door.ok(&format!("INSERT INTO ice.sales.{table} VALUES {values}"))
            .await;
    }
}

fn merge_delete(table: &str, id: i32) -> String {
    format!(
        "MERGE INTO ice.sales.{table} AS t USING (SELECT {id} AS id) AS s ON t.id = s.id \
         WHEN MATCHED THEN DELETE"
    )
}

#[tokio::test]
async fn ansi_plain_where_mor_delete_over_a_legacy_parquet_delete_merges_into_the_dv() {
    let door = door_with_session_v3_opt_in().await;
    seed_upgraded_legacy(&door, "plain", MOR_V2, &["(1, 'a'), (2, 'b'), (3, 'c')"]).await;
    door.ok(&merge_delete("plain", 2)).await;
    door.ok("ALTER TABLE ice.sales.plain SET PROPERTIES (format_version = 3)")
        .await;

    door.ok("DELETE FROM ice.sales.plain WHERE id = 3").await;

    assert_eq!(
        surviving_ids(&door, "plain").await,
        vec![1],
        "both the legacy position and the plain-WHERE one stay deleted"
    );
    assert_eq!(
        live_delete_file_kinds(&door, "plain").await,
        vec!["Puffin".to_string()],
        "V3-UPGRADE-DV-PLAIN-1 on the ANSI door: the fork's delete exec now merges the legacy \
         positions and removes the superseded parquet in the same RowDelta"
    );
}

#[tokio::test]
async fn ansi_partition_scoped_legacy_delete_merges_and_keeps_the_parquet_live() {
    let door = door_with_session_v3_opt_in().await;
    seed_upgraded_legacy(
        &door,
        "partsc",
        MOR_V2_PARTITION_GRANULARITY,
        &["(1, 'a'), (2, 'b')", "(3, 'c'), (4, 'd')"],
    )
    .await;
    door.ok(
        "MERGE INTO ice.sales.partsc AS t USING (SELECT 1 AS id UNION ALL SELECT 3) AS s \
         ON t.id = s.id WHEN MATCHED THEN DELETE",
    )
    .await;
    door.ok("ALTER TABLE ice.sales.partsc SET PROPERTIES (format_version = 3)")
        .await;

    door.ok(&merge_delete("partsc", 2)).await;

    assert_eq!(
        surviving_ids(&door, "partsc").await,
        vec![4],
        "the touched data file's DV unions the partition-scoped delete's position"
    );
    let mut kinds = live_delete_file_kinds(&door, "partsc").await;
    kinds.sort();
    assert_eq!(
        kinds,
        vec!["Parquet".to_string(), "Puffin".to_string()],
        "V3-UPGRADE-DV-PART-1 on the ANSI door: the partition-scoped parquet delete stays LIVE \
         beside the new DV, as Spark leaves it"
    );

    door.ok(&merge_delete("partsc", 4)).await;

    let mut kinds = live_delete_file_kinds(&door, "partsc").await;
    kinds.sort();
    assert_eq!(
        kinds,
        vec![
            "Parquet".to_string(),
            "Puffin".to_string(),
            "Puffin".to_string()
        ],
        "the second touched file gets its own DV and the parquet delete is STILL live"
    );
    assert!(surviving_ids(&door, "partsc").await.is_empty());
}
