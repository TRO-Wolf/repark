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
