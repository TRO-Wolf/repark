use std::collections::{HashMap, HashSet};

use datafusion::parquet::file::reader::FileReader;
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
async fn native_merge_on_read_delete_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR) WITH (\
         extra_properties = MAP(ARRAY['write.delete.mode'], ARRAY['merge-on-read']))")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("DELETE FROM ice.sales.t WHERE id = 1").await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string()), Some("a".to_string())],
    );
    assert_eq!(
        summaries[1].get("added-delete-files").map(String::as_str),
        Some("1"),
        "the delete must have committed through the row-delta arm, not a copy-on-write rewrite"
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

#[tokio::test]
async fn native_plain_update_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.t (id BIGINT, name VARCHAR)")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("UPDATE ice.sales.t SET name = 'z' WHERE id = 1")
        .await;
    let summaries = door.summaries("t").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string()), Some("a".to_string())],
        "the native door's plain UPDATE must stamp the session snapshot property like the \
         Spark door"
    );
}

#[tokio::test]
async fn native_partition_overwrite_stamps_the_session_snapshot_property() {
    let door = door_with_conf(&[("spark.sql.iceberg.snapshot-property.team", "a")]).await;
    door.ok("CREATE TABLE ice.sales.p (id BIGINT, cat VARCHAR) WITH (partitioning = ARRAY['cat'])")
        .await;
    door.ok("INSERT INTO ice.sales.p VALUES (1, 'x'), (2, 'y')")
        .await;
    door.ok("INSERT OVERWRITE ice.sales.p PARTITION (cat = 'x') SELECT 9 AS id")
        .await;
    let summaries = door.summaries("p").await;
    assert_eq!(
        ConfDoor::team_stamps(&summaries),
        vec![Some("a".to_string()), Some("a".to_string())],
        "the native door's INSERT OVERWRITE … PARTITION must stamp the session snapshot \
         property like the Spark door"
    );
}

#[tokio::test]
async fn native_partition_overwrite_takes_the_session_codec() {
    let door = door_with_conf(&[("spark.sql.iceberg.compression-codec", "gzip")]).await;
    door.ok("CREATE TABLE ice.sales.p (id BIGINT, cat VARCHAR) WITH (partitioning = ARRAY['cat'])")
        .await;
    door.ok("INSERT INTO ice.sales.p VALUES (1, 'x')").await;
    door.ok("INSERT OVERWRITE ice.sales.p PARTITION (cat = 'x') SELECT 9 AS id")
        .await;
    let table = door
        .catalog
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            "p".to_string(),
        ))
        .await
        .expect("load table");
    let codecs = live_data_file_codecs(&table).await;
    assert_eq!(
        codecs,
        vec!["GZIP(GzipLevel(6))".to_string()],
        "the native partition overwrite must write the session codec"
    );
}

async fn live_data_file_codecs(table: &iceberg::table::Table) -> Vec<String> {
    let snapshot = table.metadata().current_snapshot().expect("snapshot");
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), table.metadata())
        .await
        .expect("manifest list");
    let mut codecs = Vec::new();
    for entry in manifest_list.entries() {
        if entry.content != iceberg::spec::ManifestContentType::Data {
            continue;
        }
        let manifest = entry
            .load_manifest(table.file_io())
            .await
            .expect("manifest");
        for alive in manifest.entries().iter().filter(|entry| entry.is_alive()) {
            codecs.push(parquet_codec_of(alive.data_file().file_path()));
        }
    }
    codecs.sort();
    codecs
}

fn parquet_codec_of(path: &str) -> String {
    let local = path.strip_prefix("file://").unwrap_or(path);
    let file = std::fs::File::open(local).expect("data file");
    let reader = datafusion::parquet::file::reader::SerializedFileReader::new(file)
        .expect("parquet file reader");
    format!(
        "{:?}",
        reader.metadata().row_group(0).column(0).compression()
    )
}
