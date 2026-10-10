use std::collections::HashSet;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use iceberg::Catalog;
use repark_core::{CatalogRegistry, LocationPolicy};
use tempfile::TempDir;

use super::*;

struct AnsiDoor {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    read_only: HashSet<String>,
    catalog: Arc<dyn Catalog>,
    root: String,
    _warehouse: TempDir,
}

impl AnsiDoor {
    async fn new() -> Self {
        let warehouse = TempDir::new().unwrap();
        let root = warehouse.path().to_str().unwrap().to_string();
        let catalog: Arc<dyn Catalog> = repark_iceberg::catalog::memory_catalog(&root)
            .await
            .unwrap();
        let ctx = SessionContext::new();
        repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", Arc::clone(&catalog))
            .await
            .unwrap();
        let mut catalogs = CatalogRegistry::new();
        catalogs.insert(
            "ice".to_string(),
            Arc::clone(&catalog),
            LocationPolicy::TempFallbackAllowed {
                root: warehouse.path().to_path_buf(),
            },
        );
        catalogs.note_local_warehouse_root(&root);
        let door = Self {
            ctx,
            catalogs,
            read_only: HashSet::new(),
            catalog,
            root: root.clone(),
            _warehouse: warehouse,
        };
        door.ok(&format!(
            "CREATE SCHEMA ice.sales WITH (location = '{root}/sales')"
        ))
        .await;
        door
    }

    async fn ids(&self, sql: &str) -> datafusion::error::Result<Vec<i64>> {
        let frame = crate::execute(
            EngineContext::new(&self.ctx, &self.catalogs, &self.read_only),
            sql,
        )
        .await?;
        let batches = frame.collect().await?;
        let mut ids = Vec::new();
        for batch in &batches {
            if batch.num_columns() == 0 {
                continue;
            }
            if let Some(column) = batch
                .column(0)
                .as_any()
                .downcast_ref::<datafusion::arrow::array::Int64Array>()
            {
                for index in 0..batch.num_rows() {
                    ids.push(column.value(index));
                }
            }
        }
        Ok(ids)
    }

    async fn ok(&self, sql: &str) -> Vec<i64> {
        self.ids(sql)
            .await
            .unwrap_or_else(|error| panic!("`{sql}` must succeed: {error}"))
    }

    async fn err(&self, sql: &str) -> String {
        match self.ids(sql).await {
            Ok(_) => panic!("`{sql}` must refuse"),
            Err(error) => error.to_string(),
        }
    }

    async fn target_ids(&self) -> Vec<i64> {
        let mut ids = self.ok("SELECT id FROM ice.sales.sqtgt ORDER BY id").await;
        ids.sort_unstable();
        ids
    }
}

#[tokio::test]
async fn dml_subquery_valve_refuses_end_to_end_and_writes_nothing() {
    let door = AnsiDoor::new().await;
    door.ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    door.ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;

    for sql in [
        "UPDATE ice.sales.sqtgt SET id = 9 WHERE id NOT IN (SELECT id FROM ice.sales.sqkeys)",
        "DELETE ice.sales.sqtgt WHERE id = (SELECT max(id) FROM ice.sales.sqkeys)",
        "DELETE FROM ice.sales.sqtgt WHERE id IN (SELECT id FROM (SELECT id FROM ice.sales.sqkeys) x)",
        "DELETE FROM ice.sales.sqtgt WHERE id IN (SELECT max(id) FROM ice.sales.sqkeys)",
        "DELETE FROM ice.sales.sqtgt WHERE id = ANY (SELECT id FROM ice.sales.sqkeys)",
    ] {
        let refusal = door.err(sql).await;
        assert!(
            refusal.contains("subquery predicates are silently mis-executed"),
            "sql={sql:?}, got {refusal}"
        );
        assert_eq!(
            door.target_ids().await,
            vec![1, 2, 3],
            "a refused statement must not touch a row, sql={sql:?}"
        );
    }

    door.ok("DELETE FROM ice.sales.sqtgt WHERE id = 2").await;
    assert_eq!(door.target_ids().await, vec![1, 3]);
}

#[tokio::test]
async fn dml_subquery_in_delete_executes_and_deletes_exactly_the_match() {
    let door = AnsiDoor::new().await;
    door.ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    door.ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;

    door.ok("DELETE FROM ice.sales.sqtgt WHERE id IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(door.target_ids().await, vec![1, 3]);

    let fromless = AnsiDoor::new().await;
    fromless
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    fromless
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    fromless
        .ok("DELETE ice.sales.sqtgt WHERE id IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(fromless.target_ids().await, vec![1, 3]);
}

#[tokio::test]
async fn dml_subquery_not_in_delete_executes_and_honors_three_valued_logic() {
    let door = AnsiDoor::new().await;
    door.ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    door.ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    door.ok("DELETE FROM ice.sales.sqtgt WHERE id NOT IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(door.target_ids().await, vec![2]);

    let empty = AnsiDoor::new().await;
    empty
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    empty
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id WHERE 1 = 0")
        .await;
    empty
        .ok("DELETE FROM ice.sales.sqtgt WHERE id NOT IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(empty.target_ids().await, Vec::<i64>::new());

    let trap = AnsiDoor::new().await;
    trap.ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    trap.ok(
        "CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id UNION ALL SELECT CAST(NULL AS BIGINT)",
    )
    .await;
    trap.ok("DELETE FROM ice.sales.sqtgt WHERE id NOT IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(
        trap.target_ids().await,
        vec![1, 2, 3],
        "ANY NULL in the subquery ⇒ NOT IN matches zero rows"
    );

    let fromless = AnsiDoor::new().await;
    fromless
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    fromless
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    fromless
        .ok("DELETE ice.sales.sqtgt WHERE id NOT IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(fromless.target_ids().await, vec![2]);
}

#[tokio::test]
async fn dml_subquery_exists_delete_executes_uncorrelated_and_correlated() {
    boxed_dml_subquery_exists_delete().await;
}

fn boxed_dml_subquery_exists_delete() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()>>> {
    Box::pin(dml_subquery_exists_delete_body())
}

async fn dml_subquery_exists_delete_body() {
    let uncorrelated = AnsiDoor::new().await;
    uncorrelated
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    uncorrelated
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    uncorrelated
        .ok("DELETE FROM ice.sales.sqtgt WHERE EXISTS (SELECT 1 FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(
        uncorrelated.target_ids().await,
        Vec::<i64>::new(),
        "non-empty uncorrelated EXISTS deletes every row"
    );

    let empty = AnsiDoor::new().await;
    empty
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    empty
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id WHERE 1 = 0")
        .await;
    empty
        .ok("DELETE FROM ice.sales.sqtgt WHERE EXISTS (SELECT 1 FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(
        empty.target_ids().await,
        vec![1, 2, 3],
        "empty uncorrelated EXISTS deletes nothing"
    );

    let not_empty = AnsiDoor::new().await;
    not_empty
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    not_empty
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    not_empty
        .ok("DELETE FROM ice.sales.sqtgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(
        not_empty.target_ids().await,
        vec![1, 2, 3],
        "non-empty uncorrelated NOT EXISTS deletes nothing"
    );

    let not_vacuous = AnsiDoor::new().await;
    not_vacuous
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    not_vacuous
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id WHERE 1 = 0")
        .await;
    not_vacuous
        .ok("DELETE FROM ice.sales.sqtgt WHERE NOT EXISTS (SELECT 1 FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(
        not_vacuous.target_ids().await,
        Vec::<i64>::new(),
        "empty uncorrelated NOT EXISTS deletes every row"
    );

    let correlated = AnsiDoor::new().await;
    correlated
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    correlated
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    correlated
        .ok("DELETE FROM ice.sales.sqtgt WHERE EXISTS \
             (SELECT 1 FROM ice.sales.sqkeys k WHERE k.id = ice.sales.sqtgt.id)")
        .await;
    assert_eq!(correlated.target_ids().await, vec![1, 3]);

    let not_corr = AnsiDoor::new().await;
    not_corr
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    not_corr
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    not_corr
        .ok("DELETE FROM ice.sales.sqtgt WHERE NOT EXISTS \
             (SELECT 1 FROM ice.sales.sqkeys k WHERE k.id = ice.sales.sqtgt.id)")
        .await;
    assert_eq!(not_corr.target_ids().await, vec![2]);

    let fromless = AnsiDoor::new().await;
    fromless
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    fromless
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    fromless
        .ok("DELETE ice.sales.sqtgt WHERE EXISTS (SELECT 1 FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(fromless.target_ids().await, Vec::<i64>::new());
}

#[tokio::test]
async fn dml_subquery_correlated_in_and_update_in_execute() {
    let correlated = AnsiDoor::new().await;
    correlated
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    correlated
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    correlated
        .ok("DELETE FROM ice.sales.sqtgt WHERE id IN \
             (SELECT k.id FROM ice.sales.sqkeys k WHERE k.id = ice.sales.sqtgt.id)")
        .await;
    assert_eq!(correlated.target_ids().await, vec![1, 3]);

    let update = AnsiDoor::new().await;
    update
        .ok("CREATE TABLE ice.sales.sqtgt AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    update
        .ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;
    update
        .ok("UPDATE ice.sales.sqtgt SET id = 9 WHERE id IN (SELECT id FROM ice.sales.sqkeys)")
        .await;
    assert_eq!(update.target_ids().await, vec![1, 3, 9]);
}

#[tokio::test]
async fn mor_valve_runs_after_the_g3e8_valve() {
    use iceberg::spec::Transform;
    use repark_iceberg::write::alter::{PartitionSpecChange, apply_partition_spec_changes};

    let door = AnsiDoor::new().await;
    door.ok("CREATE TABLE ice.sales.sqtgt WITH (\
             partitioning = ARRAY['bucket(4, id)'], \
             extra_properties = MAP(ARRAY['write.delete.mode', 'write.update.mode'], \
                                    ARRAY['merge-on-read', 'merge-on-read'])) \
         AS SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3")
        .await;
    door.ok("CREATE TABLE ice.sales.sqkeys AS SELECT 2 AS id")
        .await;

    apply_partition_spec_changes(
        door.catalog.as_ref(),
        &iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new("sales".to_string()),
            "sqtgt".to_string(),
        ),
        &[PartitionSpecChange::RemoveFieldByTransform {
            source_name: "id".to_string(),
            transform: Transform::Bucket(4),
        }],
    )
    .await
    .expect("dropping the partition field must commit");
    assert!(
        door.root.starts_with('/'),
        "fixture: the warehouse is a real path"
    );

    let mor = door.err("DELETE FROM ice.sales.sqtgt WHERE id = 1").await;
    assert!(
        mor.contains("merge-on-read") && mor.contains("partition specs in history"),
        "control must be the BUG-001 message, got {mor}"
    );

    let both = door
        .err(
            "DELETE FROM ice.sales.sqtgt WHERE id IN \
             (SELECT max(id) FROM ice.sales.sqkeys)",
        )
        .await;
    assert!(
        both.contains("subquery predicates are silently mis-executed") && both.contains("G3-E8"),
        "the G3-E8 valve must fire FIRST (cheap, sync), got {both}"
    );
    assert!(
        !both.contains("partition specs in history"),
        "the BUG-001 valve must not have run, got {both}"
    );
    assert_eq!(
        door.target_ids().await,
        vec![1, 2, 3],
        "and nothing may have been written"
    );
}

#[tokio::test]
async fn collation_valve_refuses_end_to_end_and_default_select_is_untouched() {
    let door = AnsiDoor::new().await;
    let refused = door.err("SELECT 'Alice' COLLATE UTF8_LCASE").await;
    assert!(
        refused.contains(COLLATION_REFUSAL_NEEDLE),
        "end-to-end must carry the G15 needle: {refused}"
    );
    assert!(
        refused.contains("UTF8_LCASE"),
        "end-to-end must name the requested collation: {refused}"
    );
    let ids = door.ok("SELECT 1").await;
    assert_eq!(ids, vec![1], "default (non-COLLATE) SELECT must stay live");
    let cast = door
        .err("SELECT CAST('Alice' AS STRING COLLATE UTF8_LCASE)")
        .await;
    assert!(
        cast.contains(COLLATION_REFUSAL_NEEDLE) && cast.contains("UTF8_LCASE"),
        "CAST AS STRING COLLATE must be G15 end-to-end: {cast}"
    );
    let set = door
        .err("SET spark.sql.collation.objectLevel.enabled = true")
        .await;
    assert!(
        set.contains(COLLATION_REFUSAL_NEEDLE)
            && set.contains("spark.sql.collation.objectLevel.enabled"),
        "SQL SET collation key must be G15 end-to-end: {set}"
    );
}
