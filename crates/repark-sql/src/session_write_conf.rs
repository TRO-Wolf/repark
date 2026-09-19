use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use repark_iceberg::write::{
    IcebergSessionWriteConf, session_write_conf_from_config_map, with_session_write_conf,
};
use tempfile::TempDir;

struct ConfDoor {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    _warehouse_dir: TempDir,
}

impl ConfDoor {
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &read_only),
            sql,
        )
        .await?;
        frame.collect().await
    }

    async fn ok(&self, sql: &str) -> Vec<RecordBatch> {
        self.sql(sql)
            .await
            .unwrap_or_else(|err| panic!("`{sql}` must succeed: {err}"))
    }

    async fn err(&self, sql: &str) -> String {
        match self.sql(sql).await {
            Ok(_) => panic!("`{sql}` must fail"),
            Err(err) => err.to_string(),
        }
    }

    async fn summaries(&self, table: &str) -> Vec<HashMap<String, String>> {
        let table = self
            .catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("sales".to_string()),
                table.to_string(),
            ))
            .await
            .expect("load table");
        let mut snapshots: Vec<_> = table.metadata().snapshots().collect();
        snapshots.sort_by_key(|snapshot| (snapshot.timestamp_ms(), snapshot.snapshot_id()));
        snapshots
            .into_iter()
            .map(|snapshot| snapshot.summary().additional_properties.clone())
            .collect()
    }

    fn team_stamps(summaries: &[HashMap<String, String>]) -> Vec<Option<String>> {
        summaries
            .iter()
            .map(|summary| summary.get("team").cloned())
            .collect()
    }
}

fn conf_of(pairs: &[(&str, &str)]) -> IcebergSessionWriteConf {
    let map: HashMap<String, String> = pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    session_write_conf_from_config_map(&map)
}

async fn door_with_conf(pairs: &[(&str, &str)]) -> ConfDoor {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let config = with_session_write_conf(SessionConfig::new(), conf_of(pairs));
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
    let door = ConfDoor {
        ctx,
        catalogs,
        catalog,
        _warehouse_dir: warehouse_dir,
    };
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{warehouse}/sales')"
    ))
    .await;
    door
}

#[tokio::test]
async fn native_insert_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')")
        .await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string())],
        "the native door's INSERT must stamp the session snapshot property like the Spark door"
    );
}

#[tokio::test]
async fn native_ctas_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.t AS SELECT 1 AS id").await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string())],
        "the native door's CTAS must stamp the session snapshot property like the Spark door"
    );
}

#[tokio::test]
async fn native_merge_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok(
        "MERGE INTO ice.sales.t t USING (SELECT 1 AS id, 'm' AS name) s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name",
    )
    .await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string()), Some("a".to_string())],
    );
}

#[tokio::test]
async fn native_delete_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("DELETE FROM ice.sales.t WHERE id = 1").await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string()), Some("a".to_string())],
    );
}

#[tokio::test]
async fn native_insert_refuses_a_colliding_summary_key() {
    let door =
        door_with_conf(&[("spark.sql.iceberg.snapshot-property.added-records", "999")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    let message = door.err("INSERT INTO ice.sales.t VALUES (1, 'a')").await;
    assert!(
        message.contains("Multiple entries with same key: added-records=1 and added-records=999"),
        "the native door must refuse a collision with Spark's text: {message}"
    );
    assert!(
        door.summaries("t").await.is_empty(),
        "no snapshot committed"
    );
}

#[tokio::test]
async fn native_insert_stamps_a_free_summary_key() {
    let door = door_with_conf(&[
        ("spark.sql.iceberg.snapshot-property.deleted-records", "5"),
        ("spark.sql.iceberg.snapshot-property.team", "a"),
    ])
    .await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string())]
    );
    assert_eq!(
        summaries[0].get("deleted-records").map(String::as_str),
        Some("5"),
        "a key the engine did not produce is stamped"
    );
    assert_eq!(
        summaries[0].get("total-records").map(String::as_str),
        None,
        "and feeds the totals the way Java's producer does: 0 + 3 added - 5 deleted is negative, \
         so the total is dropped rather than written"
    );
}

#[tokio::test]
async fn native_bogus_session_codec_refuses_naming_the_codec() {
    let door = door_with_conf(&[("spark.sql.iceberg.compression-codec", "bogus")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    let message = door.err("INSERT INTO ice.sales.t VALUES (1, 'a')").await;
    assert!(message.contains("bogus"), "must name the codec: {message}");
    assert!(
        door.summaries("t").await.is_empty(),
        "no snapshot committed"
    );
}
