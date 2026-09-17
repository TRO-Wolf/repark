use std::sync::Arc;

use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

struct Door {
    session: ReparkSession,
    warehouse: String,
    _dir: TempDir,
}

async fn ansi_door() -> Door {
    let dir = TempDir::new().expect("warehouse");
    let warehouse = dir.path().to_str().expect("utf8").to_string();
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native session");
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .expect("catalog");
    Door {
        session,
        warehouse,
        _dir: dir,
    }
}

async fn column_names(door: &Door, sql: &str) -> Vec<String> {
    let batches = door
        .session
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("collect");
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

fn metadata_file_count(warehouse: &str) -> usize {
    let mut count = 0;
    let mut stack = vec![std::path::PathBuf::from(warehouse)];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(dir).expect("read warehouse");
        for entry in entries {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|ext| ext == "json")
                && path
                    .file_stem()
                    .is_some_and(|stem| stem.to_string_lossy().contains("metadata"))
            {
                count += 1;
            }
        }
    }
    count
}

#[tokio::test]
async fn alter_column_move_reorders_and_noop_writes_no_metadata() {
    let door = ansi_door().await;
    door.session
        .sql("CREATE SCHEMA IF NOT EXISTS ice.sales")
        .await
        .expect("schema")
        .collect()
        .await
        .expect("collect");
    door.session
        .sql("CREATE TABLE ice.sales.mv AS SELECT 1 AS id, 'a' AS a, 'b' AS b")
        .await
        .expect("create")
        .collect()
        .await
        .expect("collect");
    door.session
        .sql("ALTER TABLE ice.sales.mv ALTER COLUMN b FIRST")
        .await
        .expect("move")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        column_names(&door, "SELECT * FROM ice.sales.mv").await,
        vec!["b".to_string(), "id".to_string(), "a".to_string()]
    );
    let files_before = metadata_file_count(&door.warehouse);
    door.session
        .sql("ALTER TABLE ice.sales.mv ALTER COLUMN b FIRST")
        .await
        .expect("noop move")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        column_names(&door, "SELECT * FROM ice.sales.mv").await,
        vec!["b".to_string(), "id".to_string(), "a".to_string()]
    );
    assert_eq!(
        metadata_file_count(&door.warehouse),
        files_before + 1,
        "a move to the current position keeps the order but still commits (ICE-COLUMN-REORDER-1-R-001)"
    );
    door.session
        .sql("ALTER TABLE ice.sales.mv ALTER COLUMN id AFTER a")
        .await
        .expect("move")
        .collect()
        .await
        .expect("collect");
    assert_eq!(
        column_names(&door, "SELECT * FROM ice.sales.mv").await,
        vec!["b".to_string(), "a".to_string(), "id".to_string()]
    );
    let missing = door
        .session
        .sql("ALTER TABLE ice.sales.mv ALTER COLUMN a AFTER nope")
        .await
        .expect_err("an unknown AFTER sibling must refuse");
    let message = missing.to_string();
    assert!(
        message.contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION]")
            && message.contains("`nope`")
            && message.contains("SQLSTATE: 42703"),
        "an unknown AFTER sibling must refuse Spark-shaped, got: {message}"
    );
    let dotted = door
        .session
        .sql("ALTER TABLE ice.sales.mv ALTER COLUMN b AFTER s.a")
        .await
        .expect_err("a dotted AFTER reference must refuse");
    let dotted_message = dotted.to_string();
    assert!(
        dotted_message.contains("[PARSE_SYNTAX_ERROR]") && dotted_message.contains("42601"),
        "a dotted AFTER reference must refuse Spark-shaped, got: {dotted_message}"
    );
}
