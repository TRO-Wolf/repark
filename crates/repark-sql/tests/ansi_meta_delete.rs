use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use repark_core::{CatalogRegistry, EngineContext, LocationPolicy};
use tempfile::TempDir;

struct Door {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    _warehouse_dir: TempDir,
}

impl Door {
    async fn sql(&self, sql: &str) -> datafusion::error::Result<Vec<RecordBatch>> {
        let read_only = HashSet::new();
        let frame = repark_sql::execute(
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

    fn ident(&self, name: &str) -> TableIdent {
        TableIdent::new(NamespaceIdent::new("sales".to_string()), name.to_string())
    }

    async fn snapshots(&self, name: &str) -> Vec<(String, HashMap<String, String>)> {
        let table = self
            .catalog
            .load_table(&self.ident(name))
            .await
            .expect("load table");
        let mut ordered: Vec<_> = table.metadata().snapshots().collect();
        ordered.sort_by_key(|snapshot| snapshot.sequence_number());
        ordered
            .into_iter()
            .map(|snapshot| {
                (
                    snapshot.summary().operation.as_str().to_string(),
                    snapshot.summary().additional_properties.clone(),
                )
            })
            .collect()
    }

    async fn ids(&self, name: &str) -> Vec<i32> {
        let batches = self
            .ok(&format!("SELECT id FROM ice.sales.{name} ORDER BY id"))
            .await;
        let mut ids = Vec::new();
        for batch in &batches {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is int32");
            for index in 0..batch.num_rows() {
                ids.push(column.value(index));
            }
        }
        ids.sort_unstable();
        ids
    }
}

fn target_schema() -> IcebergSchema {
    IcebergSchema::builder()
        .with_schema_id(1)
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "v", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema")
}

async fn door(delete_mode: &str) -> Door {
    let warehouse_dir = TempDir::new().expect("warehouse tempdir");
    let warehouse = warehouse_dir
        .path()
        .to_str()
        .expect("utf8 warehouse")
        .to_string();
    let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .expect("memory catalog");
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(target_schema())
        .properties(HashMap::from([(
            "write.delete.mode".to_string(),
            delete_mode.to_string(),
        )]))
        .build();
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let ctx = SessionContext::new();
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
    let door = Door {
        ctx,
        catalogs,
        catalog,
        _warehouse_dir: warehouse_dir,
    };
    door.ok("INSERT INTO ice.sales.t VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    door.ok("INSERT INTO ice.sales.t VALUES (7, 'x')").await;
    door
}

fn summary(entry: &(String, HashMap<String, String>), key: &str) -> Option<String> {
    entry.1.get(key).cloned()
}

#[tokio::test]
async fn the_ansi_door_removes_whole_files_in_one_delete_snapshot_on_both_modes() {
    for mode in ["merge-on-read", "copy-on-write"] {
        let door = door(mode).await;
        door.ok("DELETE FROM ice.sales.t WHERE id = 7").await;
        assert_eq!(door.ids("t").await, vec![1, 2, 3], "{mode}");
        let log = door.snapshots("t").await;
        assert_eq!(log.len(), 3, "{mode}");
        let last = log.last().expect("a last snapshot");
        assert_eq!(last.0, "delete", "{mode}");
        assert_eq!(
            summary(last, "deleted-data-files").as_deref(),
            Some("1"),
            "{mode}"
        );
        assert_eq!(
            summary(last, "total-delete-files").as_deref(),
            Some("0"),
            "{mode}"
        );
        assert_eq!(summary(last, "added-delete-files"), None, "{mode}");
    }
}

#[tokio::test]
async fn the_ansi_door_commits_an_empty_delete_snapshot_on_a_no_match() {
    for mode in ["merge-on-read", "copy-on-write"] {
        let door = door(mode).await;
        door.ok("DELETE FROM ice.sales.t WHERE id = 99").await;
        assert_eq!(door.ids("t").await, vec![1, 2, 3, 7], "{mode}");
        let log = door.snapshots("t").await;
        assert_eq!(
            log.len(),
            3,
            "{mode}: a no-match DELETE is still a snapshot"
        );
        let last = log.last().expect("a last snapshot");
        assert_eq!(last.0, "delete", "{mode}");
        assert_eq!(summary(last, "deleted-data-files"), None, "{mode}");
        assert_eq!(summary(last, "added-data-files"), None, "{mode}");
        assert_eq!(
            summary(last, "total-data-files").as_deref(),
            Some("2"),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn the_ansi_door_keeps_a_partial_delete_on_the_row_level_route() {
    let door = door("merge-on-read").await;
    door.ok("DELETE FROM ice.sales.t WHERE id = 2").await;
    assert_eq!(door.ids("t").await, vec![1, 3, 7]);
    let log = door.snapshots("t").await;
    let last = log.last().expect("a last snapshot");
    assert_eq!(last.0, "delete");
    assert_eq!(summary(last, "added-delete-files").as_deref(), Some("1"));
    assert_eq!(summary(last, "deleted-data-files"), None);
}

#[tokio::test]
async fn the_ansi_door_deletes_every_file_without_a_predicate() {
    let door = door("merge-on-read").await;
    door.ok("DELETE FROM ice.sales.t").await;
    assert!(door.ids("t").await.is_empty());
    let log = door.snapshots("t").await;
    let last = log.last().expect("a last snapshot");
    assert_eq!(last.0, "delete");
    assert_eq!(summary(last, "deleted-data-files").as_deref(), Some("2"));
    assert_eq!(summary(last, "total-data-files").as_deref(), Some("0"));
}

#[tokio::test]
async fn the_ansi_door_folds_an_unquoted_column_and_stays_exact_on_a_quoted_one() {
    let door = door("merge-on-read").await;
    let refusal = door.err("DELETE FROM ice.sales.t WHERE \"ID\" = 7").await;
    assert!(
        refusal.contains("ID"),
        "a QUOTED wrong-cased column keeps the door's own refusal, got {refusal}"
    );
    assert_eq!(door.ids("t").await, vec![1, 2, 3, 7]);
    assert_eq!(door.snapshots("t").await.len(), 2);

    door.ok("DELETE FROM ice.sales.t WHERE ID = 7").await;
    assert_eq!(door.ids("t").await, vec![1, 2, 3]);
    let log = door.snapshots("t").await;
    assert_eq!(
        summary(log.last().expect("a last snapshot"), "deleted-data-files").as_deref(),
        Some("1"),
        "an UNQUOTED reference folds exactly as the door's planner folds it"
    );
}

#[tokio::test]
async fn the_ansi_door_answers_a_string_equality_from_metadata() {
    let door = door("merge-on-read").await;
    let names: Vec<String> = door
        .ok("SELECT v FROM ice.sales.t ORDER BY v")
        .await
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("v is utf8");
            (0..batch.num_rows())
                .map(|index| column.value(index).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(names, vec!["a", "b", "c", "x"]);

    door.ok("DELETE FROM ice.sales.t WHERE v = 'x'").await;
    assert_eq!(door.ids("t").await, vec![1, 2, 3]);
    let log = door.snapshots("t").await;
    assert_eq!(
        summary(log.last().expect("a last snapshot"), "deleted-data-files").as_deref(),
        Some("1")
    );
}
