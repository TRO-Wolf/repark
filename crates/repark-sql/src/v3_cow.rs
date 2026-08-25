//! Model: Claude Fable 5
//! CodeQuality:S
//!
//! ANSI-door pins: copy-on-write DML on a `register_table`-adopted format-v3 table refuses
//! (registry `V3-COW-1`, owner ruling 2026-08-25) before any write, and the table is untouched.
//!
//! The ANSI door refuses `CALL` (Q7). Adoption uses the same `Catalog::register_table` the
//! Spark procedure reaches. Memory-catalog `DROP TABLE` deletes the metadata pointer, so the
//! seed ident stays; DML runs against the adopted ident.
//!
//! pins: v3r-1-rulings/C-001, C-002, C-003, C-004, C-005

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::spec::FormatVersion;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

use crate::execute;

const COW_V3: &str = "format_version = 3, extra_properties = MAP(\
    ARRAY['write.delete.mode', 'write.update.mode', 'write.merge.mode'], \
    ARRAY['copy-on-write', 'copy-on-write', 'copy-on-write'])";

const UNSET_MERGE_V3: &str = "format_version = 3";

pub(crate) struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    pub(crate) catalog: Arc<dyn Catalog>,
    warehouse: String,
    _warehouse_dir: TempDir,
}

impl Door {
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

    async fn ok(&self, sql: &str) {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"));
    }

    pub(crate) async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    async fn table(&self, table: &str) -> iceberg::table::Table {
        self.catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .unwrap_or_else(|err| panic!("`sales.{table}` must load: {err}"))
    }

    async fn lineage(&self, table: &str) -> (u64, Option<u64>, Option<u64>) {
        let loaded = self.table(table).await;
        let metadata = loaded.metadata();
        let (first_row_id, added_rows) = metadata
            .current_snapshot()
            .map_or((None, None), |snapshot| {
                (snapshot.first_row_id(), snapshot.added_rows_count())
            });
        (metadata.next_row_id(), first_row_id, added_rows)
    }

    async fn live_pairs(&self, table: &str) -> Vec<(i32, String)> {
        let batches = self
            .sql(&format!(
                "SELECT id, name FROM ice.sales.\"{table}\" ORDER BY id"
            ))
            .await
            .unwrap_or_else(|err| panic!("select: {err}"));
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap_or_else(|| panic!("id must be Int32, got {:?}", batch.schema()));
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("name must be Utf8, got {:?}", batch.schema()));
            for index in 0..batch.num_rows() {
                rows.push((ids.value(index), names.value(index).to_string()));
            }
        }
        rows
    }
}

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

pub(crate) async fn door_with_v3_opt_in() -> Door {
    let mut config = SessionConfig::new().with_information_schema(true);
    config
        .options_mut()
        .extensions
        .insert(TestAllowCreateV3Config { allow: true });
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
    let door = Door {
        ctx,
        catalogs,
        catalog,
        warehouse,
        _warehouse_dir: warehouse_dir,
    };
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door
}

async fn adopt_v3(door: &Door, seed: &str, adopted: &str, with_clause: &str) {
    door.ok(&format!(
        "CREATE TABLE ice.sales.{seed} (id INT, name VARCHAR) WITH ({with_clause})"
    ))
    .await;
    door.ok(&format!(
        "INSERT INTO ice.sales.{seed} VALUES (1, 'a'), (2, 'b'), (3, 'c')"
    ))
    .await;
    let seed_table = door.table(seed).await;
    assert_eq!(seed_table.metadata().format_version(), FormatVersion::V3);
    let metadata_file = seed_table
        .metadata_location()
        .expect("engine-created v3 has a version-uuid pointer")
        .to_string();
    let adopted_ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        adopted.to_string(),
    );
    door.catalog
        .register_table(&adopted_ident, metadata_file)
        .await
        .unwrap_or_else(|err| panic!("register_table {adopted}: {err}"));
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        &door.ctx,
        Arc::clone(&door.catalog),
        "ice",
        &["sales"],
    )
    .await
    .expect("invalidate after register_table");
    let adopted_table = door.table(adopted).await;
    assert_eq!(
        adopted_table.metadata().format_version(),
        FormatVersion::V3,
        "register_table must keep format v3"
    );
}

async fn adopt_cow_v3(door: &Door, seed: &str, adopted: &str) {
    adopt_v3(door, seed, adopted, COW_V3).await;
}

/// Every copy-on-write arm refuses the same way, and the table keeps its snapshot, rows and
/// lineage counters.
async fn assert_cow_refused_untouched(door: &Door, table: &str, sql: &str, verb: &str) {
    let before_table = door.table(table).await;
    let before_snapshot = before_table.metadata().current_snapshot_id();
    let before = door.lineage(table).await;
    let before_rows = door.live_pairs(table).await;
    let err = door.err(sql).await;
    assert!(
        err.contains("V3-COW-1") && err.contains("row lineage") && err.contains(verb),
        "refusal must name the row, row lineage and `{verb}`: {err}"
    );
    let after_table = door.table(table).await;
    assert_eq!(
        after_table.metadata().current_snapshot_id(),
        before_snapshot,
        "a refused {verb} must not commit"
    );
    assert_eq!(after_table.metadata().format_version(), FormatVersion::V3);
    assert_eq!(
        door.lineage(table).await,
        before,
        "lineage counters untouched"
    );
    assert_eq!(door.live_pairs(table).await, before_rows, "rows untouched");
}

/// pins: v3r-1-rulings/C-001
#[tokio::test]
async fn adopted_v3_cow_delete_refuses_rather_than_reassign_row_lineage() {
    let door = door_with_v3_opt_in().await;
    adopt_cow_v3(&door, "seed_del", "adopt_del").await;
    assert_eq!(door.lineage("adopt_del").await, (3, Some(0), Some(3)));
    assert_cow_refused_untouched(
        &door,
        "adopt_del",
        "DELETE FROM ice.sales.adopt_del WHERE id = 2",
        "DELETE",
    )
    .await;
}

/// pins: v3r-1-rulings/C-002
#[tokio::test]
async fn adopted_v3_cow_update_refuses_rather_than_reassign_row_lineage() {
    let door = door_with_v3_opt_in().await;
    adopt_cow_v3(&door, "seed_upd", "adopt_upd").await;
    assert_cow_refused_untouched(
        &door,
        "adopt_upd",
        "UPDATE ice.sales.adopt_upd SET name = 'x' WHERE id = 2",
        "UPDATE",
    )
    .await;
}

/// pins: v3r-1-rulings/C-003
#[tokio::test]
async fn adopted_v3_cow_merge_refuses_with_unset_and_explicit_mode() {
    let door = door_with_v3_opt_in().await;
    adopt_v3(&door, "seed_mrg", "adopt_mrg", UNSET_MERGE_V3).await;
    assert!(
        door.table("adopt_mrg")
            .await
            .metadata()
            .properties()
            .get("write.merge.mode")
            .is_none(),
        "the unset write.merge.mode hole must be covered, not only explicit copy-on-write"
    );
    adopt_cow_v3(&door, "seed_mrg2", "adopt_mrg2").await;
    for table in ["adopt_mrg", "adopt_mrg2"] {
        assert_cow_refused_untouched(
            &door,
            table,
            &format!(
                "MERGE INTO ice.sales.{table} AS t USING (SELECT 2 AS id, \
                 CAST('m' AS VARCHAR) AS name UNION ALL SELECT 4 AS id, \
                 CAST('n' AS VARCHAR) AS name) AS s ON t.id = s.id \
                 WHEN MATCHED THEN UPDATE SET name = s.name \
                 WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
            ),
            "MERGE INTO",
        )
        .await;
    }
}

/// pins: v3r-1-rulings/C-005
#[tokio::test]
async fn v2_cow_delete_still_commits_control() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.v2del (id INT, name VARCHAR)")
        .await;
    door.ok("INSERT INTO ice.sales.v2del VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    assert_eq!(
        door.table("v2del").await.metadata().format_version(),
        FormatVersion::V2,
        "the control is a v2 table"
    );
    door.ok("DELETE FROM ice.sales.v2del WHERE id = 2").await;
    assert_eq!(
        door.live_pairs("v2del").await,
        vec![(1, "a".into()), (3, "c".into())],
        "the guard must not reach v2"
    );
}

/// pins: v3r-1-rulings/C-004
#[tokio::test]
async fn adopted_v3_mor_merge_still_refuses() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.morv3 (id INT, name VARCHAR) WITH (\
         format_version = 3, extra_properties = MAP(\
           ARRAY['write.merge.mode'], ARRAY['merge-on-read']))")
        .await;
    door.ok("INSERT INTO ice.sales.morv3 VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    let metadata_file = door
        .table("morv3")
        .await
        .metadata_location()
        .expect("pointer")
        .to_string();
    let adopted_ident = TableIdent::new(
        NamespaceIdent::new("sales".to_string()),
        "adopt_mor".to_string(),
    );
    door.catalog
        .register_table(&adopted_ident, metadata_file)
        .await
        .expect("register_table");
    repark_iceberg::catalog::invalidate_catalog_namespaces(
        &door.ctx,
        Arc::clone(&door.catalog),
        "ice",
        &["sales"],
    )
    .await
    .expect("invalidate after register_table");
    let err = door
        .err(
            "MERGE INTO ice.sales.adopt_mor AS t USING (SELECT 1 AS id, CAST('z' AS VARCHAR) AS name) AS s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .await;
    assert_eq!(
        door.table("adopt_mor").await.metadata().format_version(),
        FormatVersion::V3
    );
    assert!(
        err.contains("this table is V3") && err.contains("deletion vectors"),
        "MoR refuse must name format V3: {err}"
    );
}

/// The resolver seat: a subquery-`WHERE` `DELETE` / `UPDATE` takes the `predicate_dml` path on
/// this door, which never sees the router valve, so the refusal must come from the write-mode
/// resolver itself. Both verbs, same shape, table untouched.
///
/// pins: v3r-1-rulings/C-001, C-002
#[tokio::test]
async fn adopted_v3_cow_subquery_where_dml_refuses_at_the_resolver_seat() {
    let door = door_with_v3_opt_in().await;
    adopt_cow_v3(&door, "seed_sub", "adopt_sub").await;
    assert_cow_refused_untouched(
        &door,
        "adopt_sub",
        "DELETE FROM ice.sales.adopt_sub WHERE id IN \
         (SELECT id FROM ice.sales.adopt_sub WHERE id = 2)",
        "DELETE",
    )
    .await;
    assert_cow_refused_untouched(
        &door,
        "adopt_sub",
        "UPDATE ice.sales.adopt_sub SET name = 'x' WHERE id IN \
         (SELECT id FROM ice.sales.adopt_sub WHERE id = 2)",
        "UPDATE",
    )
    .await;
}

/// CCC SEC-001 regression (ANSI): default catalog / schema set on the session; two-part and
/// bare names refuse, table untouched.
///
/// pins: v3r-1-rulings/C-001, C-002
#[tokio::test]
async fn adopted_v3_cow_dml_with_default_catalog_short_names_refuses() {
    let door = door_with_v3_opt_in().await;
    adopt_cow_v3(&door, "seed_short", "adopt_short").await;
    door.ctx
        .sql("SET datafusion.catalog.default_catalog = 'ice'")
        .await
        .expect("set default catalog");
    door.ctx
        .sql("SET datafusion.catalog.default_schema = 'sales'")
        .await
        .expect("set default schema");
    assert_cow_refused_untouched(
        &door,
        "adopt_short",
        "DELETE FROM sales.adopt_short WHERE id = 2",
        "DELETE",
    )
    .await;
    assert_cow_refused_untouched(
        &door,
        "adopt_short",
        "UPDATE adopt_short SET name = 'x' WHERE id = 2",
        "UPDATE",
    )
    .await;
}

/// CCC SEC-003 regression (ANSI): a dotted quoted table name — `ice.sales."a.b"` is creatable
/// on this door — broke the text scraper the valve used, so the load failed, the valve stepped
/// aside and the DELETE committed a v3 rewrite. The target now comes from the AST.
///
/// pins: v3r-1-rulings/C-001
#[tokio::test]
async fn adopted_v3_cow_delete_on_a_dotted_quoted_name_refuses() {
    let door = door_with_v3_opt_in().await;
    door.ok("CREATE TABLE ice.sales.\"a.b\" (id INT, name VARCHAR) WITH (format_version = 3)")
        .await;
    door.ok("INSERT INTO ice.sales.\"a.b\" VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    assert_cow_refused_untouched(
        &door,
        "a.b",
        "DELETE FROM ice.sales.\"a.b\" WHERE id = 2",
        "DELETE",
    )
    .await;
}

/// CCC SEC-002 regression (ANSI): padded merge-on-read spelling still refuses on v3.
///
/// pins: v3r-1-rulings/C-004
#[tokio::test]
async fn adopted_v3_padded_merge_on_read_spelling_still_refuses() {
    let door = door_with_v3_opt_in().await;
    adopt_v3(
        &door,
        "seed_pad",
        "adopt_pad",
        "format_version = 3, extra_properties = MAP(ARRAY['write.delete.mode'], \
         ARRAY[' Merge-On-Read '])",
    )
    .await;
    let before = door.lineage("adopt_pad").await;
    let err = door
        .err("DELETE FROM ice.sales.adopt_pad WHERE id = 2")
        .await;
    assert!(
        err.contains("V3") && err.contains("deletion vectors"),
        "the merge-on-read arm's reason: {err}"
    );
    assert_eq!(door.lineage("adopt_pad").await, before, "no commit");
    assert_eq!(
        door.live_pairs("adopt_pad").await,
        vec![(1, "a".into()), (2, "b".into()), (3, "c".into())]
    );
}
