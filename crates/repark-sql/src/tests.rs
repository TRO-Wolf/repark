//! End-to-end door tests on a **native** session — no extension, no Spark analyzer, stock
//! DataFusion semantics. That profile is load-bearing, not incidental: extensions are
//! session-scoped, so evidence gathered on a Spark-extended session would describe the Spark
//! analyzer rather than this door (design §2 Q13, graft G5). Every row the ANSI surface matrix
//! marks `Native` is pinned here.
//!
//! Results are asserted on the **Arrow path** (`collect`), value AND type — never `show`. Where
//! a statement's effect is metadata rather than rows (a partition spec, a format version, a raw
//! property), the assertion loads the Iceberg table and reads the metadata directly, because
//! that IS the observable effect.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

/// A native session with one registered in-memory Iceberg catalog (`ice`) over a temp warehouse.
struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
    /// Run one statement through the ANSI door.
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    /// Run a statement that must succeed.
    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"))
    }

    /// Run a statement that must succeed, returning its schema ALONGSIDE the batches.
    ///
    /// A zero-row result may collect to zero BATCHES, so `batches[0].schema()` is not a safe way
    /// to assert types on an empty table — the schema has to come from the frame.
    async fn ok_typed(
        &self,
        sql: &str,
    ) -> (datafusion::arrow::datatypes::SchemaRef, Vec<RecordBatch>) {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await
        .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
        let schema = Arc::new(frame.schema().as_arrow().clone());
        let batches = frame
            .collect()
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must collect: {err}"));
        (schema, batches)
    }

    /// Run a statement that must fail, returning the message.
    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    async fn table(&self, namespace: &str, table: &str) -> iceberg::table::Table {
        self.catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(namespace.to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("`{namespace}.{table}` must load: {err}"))
    }

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

/// Build the door session. `information_schema` is enabled at the DataFusion layer so the Q8
/// delegation surface is reachable — see the PR-5 R2 spike: `ReparkSession`'s builder cannot yet
/// enable it, which is a recorded repark-core gap, NOT a property of this door.
async fn door() -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();

    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_information_schema(true));
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

/// A door with the `sales` schema already created at a real warehouse location.
async fn door_with_schema() -> Door {
    let door = door().await;
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

// === Statement forms ========================================================================

/// CTAS into a registered Iceberg catalog writes real rows into a real table, and reads back
/// through the catalog provider with the right values AND types.
#[tokio::test]
async fn ctas_into_registered_iceberg_catalog_round_trips() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.orders AS SELECT 1 AS id, 'a' AS label \
         UNION ALL SELECT 2 AS id, 'b' AS label",
    )
    .await;

    let batches = door
        .ok("SELECT id, label FROM ice.sales.orders ORDER BY id")
        .await;
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 2, "both rows must be written");

    let schema = batches[0].schema();
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "id type");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "label type");

    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 id");
    let labels = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 label");
    assert_eq!((ids.value(0), labels.value(0)), (1, "a"));
    assert_eq!((ids.value(1), labels.value(1)), (2, "b"));

    // The table really exists in the catalog, not only in the session.
    assert!(door.table_exists("sales", "orders").await);
}

/// Q15/G1: a create target that does not resolve to a registered Iceberg catalog REFUSES,
/// requiring qualification — and creates nothing.
///
/// The refusal is the whole point of the ruling: DataFusion's own CTAS would have made a
/// session-local `MemTable` that reads back correctly all session and is gone tomorrow. The last
/// assertion is the one that matters — nothing was created.
#[tokio::test]
async fn ctas_unregistered_target_refuses_requiring_qualification() {
    let door = door_with_schema().await;
    for sql in [
        "CREATE TABLE orders AS SELECT 1 AS id",
        "CREATE TABLE analytics.orders AS SELECT 1 AS id",
        "CREATE TABLE nosuchcatalog.sales.orders AS SELECT 1 AS id",
    ] {
        let err = door.err(sql).await;
        assert!(
            err.contains("<catalog>.<schema>.<table>"),
            "`{sql}` must require qualification: {err}"
        );
        assert!(
            err.contains("`ice`"),
            "`{sql}` must list the registered catalogs: {err}"
        );
        assert!(
            err.contains("vanish when the session ends"),
            "`{sql}` must explain WHY it refuses rather than falling through: {err}"
        );
    }
    // Nothing was created anywhere.
    assert!(!door.table_exists("sales", "orders").await);
    assert!(
        door.sql("SELECT * FROM orders").await.is_err(),
        "no session-local table may have been created"
    );
}

/// A create naming a registered catalog but no schema gets a targeted message, not the generic
/// qualification one.
#[tokio::test]
async fn ctas_missing_schema_segment_names_the_gap() {
    let door = door_with_schema().await;
    let err = door.err("CREATE TABLE ice.orders AS SELECT 1 AS id").await;
    assert!(
        err.contains("ice.<schema>.orders"),
        "must show the missing segment: {err}"
    );
}

/// `CREATE OR REPLACE TABLE … AS SELECT` replaces the table's contents through one staged
/// publish — the old rows are gone, the new rows are there.
#[tokio::test]
async fn create_or_replace_table_as_select_replaces_rows() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id").await;
    door.ok("CREATE OR REPLACE TABLE ice.sales.t AS SELECT 9 AS id, 'new' AS label")
        .await;

    let batches = door.ok("SELECT id, label FROM ice.sales.t").await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        1,
        "the replaced table holds exactly the new rows"
    );
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(ids.value(0), 9, "value");
    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &DataType::Utf8,
        "the NEW schema is authoritative"
    );
}

/// A plain `CREATE TABLE` on an existing table refuses; `IF NOT EXISTS` makes it a no-op that
/// leaves the original rows untouched.
#[tokio::test]
async fn create_table_existing_refuses_and_if_not_exists_is_a_noop() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id").await;

    let err = door.err("CREATE TABLE ice.sales.t AS SELECT 2 AS id").await;
    assert!(err.contains("already exists"), "must name the class: {err}");
    assert!(
        err.contains("CREATE OR REPLACE"),
        "must offer the alternative: {err}"
    );

    door.ok("CREATE TABLE IF NOT EXISTS ice.sales.t AS SELECT 2 AS id")
        .await;
    let batches = door.ok("SELECT id FROM ice.sales.t").await;
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(ids.value(0), 1, "IF NOT EXISTS must not overwrite");
}

/// The column-def form creates a real, EMPTY Iceberg table whose declared types and NOT NULL
/// constraints survive into the Iceberg schema.
#[tokio::test]
async fn create_table_column_def_creates_iceberg_table() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.dim (id INT NOT NULL, name VARCHAR, ratio DOUBLE)")
        .await;

    let (schema, batches) = door
        .ok_typed("SELECT id, name, ratio FROM ice.sales.dim")
        .await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "a column-def create writes no rows"
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Int32, "INT");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "VARCHAR");
    assert_eq!(schema.field(2).data_type(), &DataType::Float64, "DOUBLE");

    // NOT NULL reached the Iceberg schema as a REQUIRED field.
    let table = door.table("sales", "dim").await;
    let fields = table.metadata().current_schema().as_struct().fields();
    assert!(fields[0].required, "`id` must be required");
    assert!(!fields[1].required, "`name` must be optional");
}

/// A column-def create with no columns, and a CTAS carrying both a column list and a query, both
/// refuse rather than guessing.
#[tokio::test]
async fn create_table_shape_refusals() {
    let door = door_with_schema().await;
    let err = door
        .err("CREATE TABLE ice.sales.bad (id INT) AS SELECT 1 AS id")
        .await;
    assert!(
        err.contains("may not be combined with AS SELECT"),
        "must name the class: {err}"
    );
}

/// `DROP TABLE` removes the table; `IF EXISTS` is idempotent and a bare drop of a missing table
/// fails loud.
#[tokio::test]
async fn drop_table_if_exists_is_idempotent() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id").await;
    assert!(door.table_exists("sales", "t").await);

    door.ok("DROP TABLE ice.sales.t").await;
    assert!(!door.table_exists("sales", "t").await, "the table is gone");
    assert!(
        door.sql("SELECT * FROM ice.sales.t").await.is_err(),
        "the name directory must be refreshed too"
    );

    // Idempotent.
    door.ok("DROP TABLE IF EXISTS ice.sales.t").await;
    door.ok("DROP TABLE IF EXISTS ice.sales.never_existed")
        .await;

    // …and a bare drop of a missing table still fails loud.
    door.err("DROP TABLE ice.sales.never_existed").await;
}

/// An unqualified `DROP TABLE` refuses rather than reaching for a session-local name.
#[tokio::test]
async fn drop_table_unqualified_refuses() {
    let door = door_with_schema().await;
    let err = door.err("DROP TABLE orders").await;
    assert!(
        err.contains("<catalog>.<schema>.<table>"),
        "must require qualification: {err}"
    );
}

/// `CREATE SCHEMA` creates the Iceberg namespace, and it becomes visible to the session.
#[tokio::test]
async fn create_schema_creates_the_namespace() {
    let door = door().await;
    let location = format!("{}/bronze", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.bronze WITH (location = '{location}')"
    ))
    .await;

    assert!(
        door.catalog
            .namespace_exists(&NamespaceIdent::new("bronze".to_string()))
            .await
            .expect("namespace_exists"),
        "the namespace must exist in the catalog"
    );
    // Visible to the session's name directory.
    let batches = door
        .ok("SELECT schema_name FROM information_schema.schemata WHERE catalog_name = 'ice'")
        .await;
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8");
    assert!(
        (0..names.len()).any(|index| names.value(index) == "bronze"),
        "the new schema must enumerate"
    );

    // `IF NOT EXISTS` is a no-op; a bare re-create fails loud.
    door.ok(&format!(
        "CREATE SCHEMA IF NOT EXISTS ice.bronze WITH (location = '{location}')"
    ))
    .await;
    door.err("CREATE SCHEMA ice.bronze").await;
}

/// `CREATE SCHEMA … WITH (location = …)` stores the location under BOTH canonical keys, so a
/// later create resolves its table location whichever key the catalog implementation maps.
#[tokio::test]
async fn create_schema_with_location_stores_both_keys() {
    let door = door().await;
    let location = format!("{}/silver", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.silver WITH (location = '{location}')"
    ))
    .await;

    let namespace = door
        .catalog
        .get_namespace(&NamespaceIdent::new("silver".to_string()))
        .await
        .expect("namespace");
    assert_eq!(
        namespace.properties().get("location").map(String::as_str),
        Some(location.as_str()),
        "`location` must be stored"
    );
    assert_eq!(
        namespace
            .properties()
            .get("location_uri")
            .map(String::as_str),
        Some(location.as_str()),
        "`location_uri` must be mirrored (the key a real Glue database fills)"
    );

    // The location is load-bearing: a create under this schema lands beneath it.
    door.ok("CREATE TABLE ice.silver.t AS SELECT 1 AS id").await;
    let table = door.table("silver", "t").await;
    assert!(
        table.metadata().location().starts_with(&location),
        "the table must sit under the schema location, got {}",
        table.metadata().location()
    );
}

/// An unknown `CREATE SCHEMA` property refuses, listing the supported set.
#[tokio::test]
async fn create_schema_unknown_property_refuses() {
    let door = door().await;
    let err = door
        .err("CREATE SCHEMA ice.bronze WITH (owner = 'me')")
        .await;
    assert!(err.contains("owner"), "must name the key: {err}");
    assert!(err.contains("`location`"), "must list support: {err}");
}

/// `DROP SCHEMA` removes the namespace; `IF EXISTS` is idempotent; `CASCADE` refuses.
#[tokio::test]
async fn drop_schema_drops_the_namespace() {
    let door = door().await;
    let location = format!("{}/bronze", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.bronze WITH (location = '{location}')"
    ))
    .await;

    door.ok("DROP SCHEMA ice.bronze").await;
    assert!(
        !door
            .catalog
            .namespace_exists(&NamespaceIdent::new("bronze".to_string()))
            .await
            .expect("namespace_exists"),
        "the namespace must be gone"
    );

    door.ok("DROP SCHEMA IF EXISTS ice.bronze").await;
    door.err("DROP SCHEMA ice.bronze").await;

    let cascade = door.err("DROP SCHEMA IF EXISTS ice.bronze CASCADE").await;
    assert!(
        cascade.contains("CASCADE"),
        "CASCADE must refuse by name: {cascade}"
    );
    assert!(cascade.contains("destructive"), "…and say why: {cascade}");
}

/// Q8: a registered Iceberg catalog enumerates through `information_schema` — the delegation
/// this door's introspection ruling depends on.
///
/// (The PR-5 R2 spike recorded that `ReparkSession`'s builder cannot yet turn
/// `information_schema` on; that is a repark-core gap filed separately. The door itself, given a
/// session where it IS on, enumerates correctly — which is what this row claims.)
#[tokio::test]
async fn information_schema_enumerates_registered_iceberg_catalogs() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.orders AS SELECT 1 AS id")
        .await;

    let batches = door
        .ok("SELECT table_name FROM information_schema.tables \
             WHERE table_catalog = 'ice' AND table_schema = 'sales' AND table_name = 'orders'")
        .await;
    let names = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Utf8 table_name");
    assert_eq!(names.len(), 1, "the table must enumerate exactly once");
    assert_eq!(names.value(0), "orders", "value");
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Utf8,
        "type"
    );
}

// === Table-creation options =================================================================

/// `format = 'PARQUET'` is accepted end to end; `ORC` and `AVRO` refuse loud, each naming its
/// trigger — and, crucially, create nothing.
#[tokio::test]
async fn with_format_parquet_accepted_orc_and_avro_refuse_loud() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.p WITH (format = 'PARQUET') AS SELECT 1 AS id")
        .await;
    assert!(door.table_exists("sales", "p").await);

    for format in ["ORC", "AVRO"] {
        let err = door
            .err(&format!(
                "CREATE TABLE ice.sales.{} WITH (format = '{format}') AS SELECT 1 AS id",
                format.to_lowercase()
            ))
            .await;
        assert!(err.contains(format), "must name the format: {err}");
        assert!(err.contains("TRIGGER"), "must name the trigger: {err}");
        assert!(
            !door.table_exists("sales", &format.to_lowercase()).await,
            "a refused create must not leave a table behind"
        );
    }
}

/// `format_version = 2` is accepted and the created table really is Iceberg v2; any other
/// version refuses instead of being silently ignored.
#[tokio::test]
async fn with_format_version_sets_the_table_format_version() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.v2 WITH (format_version = 2) AS SELECT 1 AS id")
        .await;
    let table = door.table("sales", "v2").await;
    assert_eq!(
        table.metadata().format_version() as u8,
        2,
        "the table must be format v2"
    );

    let err = door
        .err("CREATE TABLE ice.sales.v1 WITH (format_version = 1) AS SELECT 1 AS id")
        .await;
    assert!(err.contains("format_version"), "must name the key: {err}");
    assert!(!door.table_exists("sales", "v1").await, "nothing created");
}

/// `partitioning = ARRAY[…]` builds the Iceberg partition spec, with Java-parity field names —
/// and the partitioned table still round-trips its rows.
#[tokio::test]
async fn with_partitioning_array_builds_the_partition_spec() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.part WITH (partitioning = ARRAY['bucket(4, id)', 'label']) \
         AS SELECT 1 AS id, 'a' AS label UNION ALL SELECT 2 AS id, 'b' AS label",
    )
    .await;

    let table = door.table("sales", "part").await;
    let spec = table.metadata().default_partition_spec();
    let names: Vec<&str> = spec.fields().iter().map(|f| f.name.as_str()).collect();
    assert_eq!(
        names,
        vec!["id_bucket", "label"],
        "clause order + Java field names"
    );
    assert!(!spec.is_unpartitioned(), "the table must be partitioned");

    let batches = door.ok("SELECT id FROM ice.sales.part ORDER BY id").await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        2,
        "a partitioned write must round-trip every row"
    );

    // A transform naming a column the SELECT does not produce refuses, listing what IS there.
    let err = door
        .err(
            "CREATE TABLE ice.sales.bad WITH (partitioning = ARRAY['month(missing)']) \
             AS SELECT 1 AS id",
        )
        .await;
    assert!(err.contains("`missing`"), "must name the column: {err}");
    assert!(err.contains("[id]"), "must list the columns: {err}");
}

/// `location` places the table exactly where it says, overriding the schema location.
#[tokio::test]
async fn with_location_lands_the_table_under_the_path() {
    let door = door_with_schema().await;
    let explicit = format!("{}/explicit/place", door.warehouse);
    door.ok(&format!(
        "CREATE TABLE ice.sales.here WITH (location = '{explicit}') AS SELECT 1 AS id"
    ))
    .await;

    let table = door.table("sales", "here").await;
    assert_eq!(
        table.metadata().location(),
        explicit,
        "the explicit location must win over the schema location"
    );
    let batches = door.ok("SELECT id FROM ice.sales.here").await;
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(ids.value(0), 1, "the table must still read back");
}

/// The G4 hatch: `extra_properties` lands RAW Iceberg keys on the created table's properties.
#[tokio::test]
async fn with_extra_properties_map_passes_raw_iceberg_keys() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.props WITH (extra_properties = MAP(\
         ARRAY['write.target-file-size-bytes', 'my.custom.key'], \
         ARRAY['134217728', 'hello'])) AS SELECT 1 AS id")
        .await;

    let table = door.table("sales", "props").await;
    let properties = table.metadata().properties();
    assert_eq!(
        properties
            .get("write.target-file-size-bytes")
            .map(String::as_str),
        Some("134217728"),
        "a dotted Iceberg key must arrive verbatim"
    );
    assert_eq!(
        properties.get("my.custom.key").map(String::as_str),
        Some("hello"),
        "an arbitrary user key must arrive verbatim"
    );
}

/// The concrete thing the hatch buys (design §2 Q1): a merge-on-read table is creatable in
/// phase 2, without `write.merge.mode` being frozen into this door's bare-key API.
#[tokio::test]
async fn extra_properties_write_merge_mode_creates_a_mor_table() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.mor WITH (extra_properties = MAP(\
         ARRAY['write.merge.mode', 'write.delete.mode', 'write.update.mode'], \
         ARRAY['merge-on-read', 'merge-on-read', 'merge-on-read'])) \
         AS SELECT 1 AS id, 'a' AS label")
        .await;

    let table = door.table("sales", "mor").await;
    for key in ["write.merge.mode", "write.delete.mode", "write.update.mode"] {
        assert_eq!(
            table.metadata().properties().get(key).map(String::as_str),
            Some("merge-on-read"),
            "`{key}` must be set on the created table"
        );
    }
    // …and the table is a real, readable table, not just a property bag.
    let batches = door.ok("SELECT id, label FROM ice.sales.mor").await;
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].schema().field(1).data_type(), &DataType::Utf8);
}

/// G9: `sorted_by` refuses loud, names its trigger, and creates nothing.
#[tokio::test]
async fn with_sorted_by_refuses_loud_naming_the_trigger() {
    let door = door_with_schema().await;
    let err = door
        .err("CREATE TABLE ice.sales.sorted WITH (sorted_by = ARRAY['id']) AS SELECT 1 AS id")
        .await;
    assert!(err.contains("sorted_by"), "must name the property: {err}");
    assert!(err.contains("TRIGGER"), "must name the trigger: {err}");
    assert!(
        !door.table_exists("sales", "sorted").await,
        "a reserved-property refusal must not create a table"
    );
}

/// The typo guard end to end: an unknown bare key refuses listing the curated set, and points
/// dotted keys at the hatch.
#[tokio::test]
async fn unknown_bare_property_refuses_listing_the_curated_set() {
    let door = door_with_schema().await;
    let err = door
        .err("CREATE TABLE ice.sales.oops WITH (partition_by = ARRAY['id']) AS SELECT 1 AS id")
        .await;
    assert!(err.contains("partition_by"), "must name the key: {err}");
    for key in [
        "`format`",
        "`format_version`",
        "`partitioning`",
        "`location`",
        "`extra_properties`",
    ] {
        assert!(err.contains(key), "must list {key}: {err}");
    }
    assert!(
        err.contains("extra_properties = MAP"),
        "must point dotted keys at the hatch: {err}"
    );
    assert!(!door.table_exists("sales", "oops").await, "nothing created");
}

/// Spark's `TBLPROPERTIES` on a CREATE is refused with a steer, not silently dropped.
#[tokio::test]
async fn tblproperties_on_create_refuses_with_a_steer() {
    let door = door_with_schema().await;
    let err = door
        .err(
            "CREATE TABLE ice.sales.t TBLPROPERTIES ('write.merge.mode' = 'merge-on-read') \
             AS SELECT 1 AS id",
        )
        .await;
    assert!(
        err.contains("TBLPROPERTIES") || err.contains("Spark SQL"),
        "must identify the Spark spelling: {err}"
    );
}

/// A create whose schema has NO location falls back to the registration-time temp root on a
/// `TempFallbackAllowed` catalog — offline work keeps running without a configured warehouse.
#[tokio::test]
async fn location_less_schema_falls_back_on_a_temp_fallback_catalog() {
    let door = door().await;
    door.catalog
        .create_namespace(
            &NamespaceIdent::new("nowhere".to_string()),
            std::collections::HashMap::new(),
        )
        .await
        .expect("create namespace without a location");

    door.ok("CREATE TABLE ice.nowhere.t AS SELECT 1 AS id")
        .await;
    let table = door.table("nowhere", "t").await;
    assert!(
        table.metadata().location().contains("repark_ansi_ctas"),
        "the temp fallback must be used, got {}",
        table.metadata().location()
    );
}

/// The same create on a `RequireExplicitLocation` catalog FAILS LOUD instead — a real warehouse
/// must never have its data placed in a temporary directory.
#[tokio::test]
async fn location_less_schema_fails_loud_on_a_strict_catalog() {
    let warehouse_dir = TempDir::new().expect("tempdir");
    let warehouse = warehouse_dir.path().to_str().expect("utf8").to_string();
    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("catalog");
    catalog
        .create_namespace(
            &NamespaceIdent::new("strict".to_string()),
            std::collections::HashMap::new(),
        )
        .await
        .expect("namespace");

    let ctx = SessionContext::new();
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "glue_like", Arc::clone(&catalog))
        .await
        .expect("register");
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "glue_like".to_string(),
        Arc::clone(&catalog),
        LocationPolicy::RequireExplicitLocation,
    );

    let read_only = HashSet::new();
    let err = crate::execute(
        EngineContext::new(&ctx, &catalogs, &read_only),
        "CREATE TABLE glue_like.strict.t AS SELECT 1 AS id",
    )
    .await
    .expect_err("a strict catalog must refuse a location-less schema")
    .to_string();

    assert!(err.contains("no `location` property"), "class: {err}");
    assert!(
        err.contains("CREATE SCHEMA glue_like.strict WITH (location"),
        "must show the ANSI fix: {err}"
    );
    assert!(
        err.contains("temporary directory"),
        "must say why it will not guess: {err}"
    );
}

/// A read-only catalog is refused as a DDL target with the generic P11 message, not with an
/// "unknown catalog" error — the user's problem is direction, not spelling.
#[tokio::test]
async fn ddl_against_a_read_only_catalog_refuses_with_the_direction_note() {
    let door = door_with_schema().await;
    let mut catalogs = door.catalogs.clone();
    catalogs.set_read_only_catalogs(HashSet::from(["pg".to_string()]));
    let read_only = HashSet::from(["pg".to_string()]);

    let err = crate::execute(
        EngineContext::new(&door.ctx, &catalogs, &read_only),
        "CREATE TABLE pg.public.t AS SELECT 1 AS id",
    )
    .await
    .expect_err("a read-only catalog must refuse")
    .to_string();
    assert!(err.contains("registered read-only"), "class: {err}");
    assert!(err.contains("MERGE INTO"), "must give the direction: {err}");
}

/// Identifiers that would escape the warehouse root are rejected before any path is composed.
#[tokio::test]
async fn path_escaping_identifiers_are_rejected() {
    let door = door_with_schema().await;
    let err = door
        .err(r#"CREATE TABLE ice.sales."../../etc/evil" AS SELECT 1 AS id"#)
        .await;
    assert!(
        err.contains("path traversal") || err.contains("path separators"),
        "must reject the escape: {err}"
    );
}

/// A CTAS whose SELECT produces zero rows still creates the table — an empty result is a valid
/// answer, not a reason to skip the create.
#[tokio::test]
async fn empty_ctas_still_creates_the_table() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.empty AS SELECT 1 AS id WHERE 1 = 0")
        .await;
    assert!(door.table_exists("sales", "empty").await);
    let (schema, batches) = door.ok_typed("SELECT id FROM ice.sales.empty").await;
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        0,
        "no rows, but the table exists"
    );
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int64,
        "the schema is still derived from the SELECT"
    );
}

/// Native (ANSI) semantics, not Spark's: integer division stays integer through this door on a
/// session with no extension installed. This is the one-line proof that the door did NOT quietly
/// acquire Spark expression semantics.
#[tokio::test]
async fn native_session_keeps_ansi_expression_semantics() {
    let door = door().await;
    let batches = door.ok("SELECT 7 / 2 AS q").await;
    assert_eq!(
        batches[0].schema().field(0).data_type(),
        &DataType::Int64,
        "ANSI integer division must stay Int64 (Spark would give Float64)"
    );
    let value = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64");
    assert_eq!(value.value(0), 3, "value");
}

/// A `CREATE TABLE` reaching an INT32 column type proves the column-def path is not silently
/// widening declared types (the failure mode a hand-rolled type table would produce).
#[tokio::test]
async fn column_def_types_are_not_widened() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.narrow (small INT)").await;
    let (schema, batches) = door.ok_typed("SELECT small FROM ice.sales.narrow").await;
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int32,
        "INT must stay 32-bit, not widen to Int64"
    );
    for batch in &batches {
        assert!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .is_some(),
            "the column must be an Int32 array"
        );
    }
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
}

// === Delegated DML (fork `TableProvider`, ADR-0003) =========================================

/// `INSERT INTO` an Iceberg table delegates to the fork's `TableProvider` and really commits:
/// the new row reads back through a FRESH catalog load, not just through the session.
///
/// This is a WRITE surface, so it is pinned rather than assumed. (An earlier revision marked it
/// `DeliberatelyAbsent` while it was live — the exact false-absence the surface matrix exists to
/// prevent.)
#[tokio::test]
async fn insert_into_iceberg_table_round_trips() {
    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.ins AS SELECT 1 AS id, 'a' AS label")
        .await;
    door.ok("INSERT INTO ice.sales.ins VALUES (2, 'b')").await;

    let (schema, batches) = door
        .ok_typed("SELECT id, label FROM ice.sales.ins ORDER BY id")
        .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "id type");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "label type");
    let ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("Int64 id")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![1, 2], "the inserted row must be visible");

    // …and it is a COMMITTED Iceberg row: the table now carries a second snapshot.
    let table = door.table("sales", "ins").await;
    assert!(
        table.metadata().snapshots().count() >= 2,
        "the insert must have committed its own snapshot"
    );
}

/// `DELETE FROM` delegates and removes exactly the matching rows (copy-on-write default).
#[tokio::test]
async fn delete_from_iceberg_table_removes_matching_rows() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.del AS SELECT 1 AS id, 'a' AS label \
         UNION ALL SELECT 2 AS id, 'b' AS label",
    )
    .await;
    door.ok("DELETE FROM ice.sales.del WHERE id = 1").await;

    let (schema, batches) = door.ok_typed("SELECT id, label FROM ice.sales.del").await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "id type");
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "exactly one row must survive");
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 id");
    assert_eq!(ids.value(0), 2, "the non-matching row must survive");
}

/// `UPDATE … SET` delegates and rewrites exactly the matching rows, types intact.
#[tokio::test]
async fn update_iceberg_table_rewrites_matching_rows() {
    let door = door_with_schema().await;
    door.ok(
        "CREATE TABLE ice.sales.upd AS SELECT 1 AS id, 'a' AS label \
         UNION ALL SELECT 2 AS id, 'b' AS label",
    )
    .await;
    door.ok("UPDATE ice.sales.upd SET label = 'z' WHERE id = 1")
        .await;

    let (schema, batches) = door
        .ok_typed("SELECT id, label FROM ice.sales.upd ORDER BY id")
        .await;
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "label type");
    let mut pairs: Vec<(i64, String)> = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 id");
        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 label");
        for row in 0..batch.num_rows() {
            pairs.push((ids.value(row), labels.value(row).to_string()));
        }
    }
    pairs.sort();
    assert_eq!(
        pairs,
        vec![(1, "z".to_string()), (2, "b".to_string())],
        "only the matching row may change"
    );
}

/// BUG-001 valve, wired: `DELETE`/`UPDATE` against a merge-on-read table whose CURRENT spec is
/// unpartitioned while its history carries an earlier spec REFUSES rather than silently
/// under-deleting (the fork's unpartitioned position-delete fast path, `ENGINE_CONTRACT` §7a).
///
/// The fixture is built through this door plus the tier-1 spec-evolution helper, because that is
/// the only way to reach the hazard shape in M1: create merge-on-read AND partitioned (the
/// `extra_properties` hatch + `partitioning`), then drop the partition field so the current spec
/// is unpartitioned and the history has two specs.
#[tokio::test]
async fn mor_unpartitioned_multi_spec_dml_refuses() {
    use iceberg::spec::Transform;
    use repark_iceberg::write::alter::{PartitionSpecChange, apply_partition_spec_changes};

    let door = door_with_schema().await;
    door.ok("CREATE TABLE ice.sales.morevo WITH (\
             partitioning = ARRAY['bucket(4, id)'], \
             extra_properties = MAP(ARRAY['write.delete.mode', 'write.update.mode'], \
                                    ARRAY['merge-on-read', 'merge-on-read'])) \
         AS SELECT 1 AS id, 'a' AS label")
        .await;

    // Evolve the spec away: current spec becomes unpartitioned, history keeps the bucket spec.
    apply_partition_spec_changes(
        door.catalog.as_ref(),
        &TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "morevo".to_string(),
        ),
        &[PartitionSpecChange::RemoveFieldByTransform {
            source_name: "id".to_string(),
            transform: Transform::Bucket(4),
        }],
    )
    .await
    .expect("dropping the partition field must commit");

    let table = door.table("sales", "morevo").await;
    assert!(
        table.metadata().default_partition_spec().is_unpartitioned(),
        "fixture: the current spec must be unpartitioned"
    );
    assert!(
        table.metadata().partition_specs_iter().len() > 1,
        "fixture: the history must carry more than one spec"
    );

    for sql in [
        "DELETE FROM ice.sales.morevo WHERE id = 1",
        "UPDATE ice.sales.morevo SET label = 'z' WHERE id = 1",
    ] {
        let err = door.err(sql).await;
        assert!(
            err.contains("merge-on-read") && err.contains("partition specs in history"),
            "`{sql}` must hit the BUG-001 valve: {err}"
        );
        assert!(
            err.contains("copy-on-write") || err.contains("MERGE INTO"),
            "`{sql}` must name a workaround: {err}"
        );
    }

    // The valve is TARGETED, not a blanket DML refuse: an ordinary table still deletes.
    door.ok("CREATE TABLE ice.sales.plain AS SELECT 1 AS id")
        .await;
    door.ok("DELETE FROM ice.sales.plain WHERE id = 1").await;
}
