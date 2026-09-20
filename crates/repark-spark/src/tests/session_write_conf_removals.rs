use super::super::*;
use super::common::*;
use super::session_write_conf::{set_session_conf, unset_session_conf};

async fn overwrite_refusal(seed: &[String], key: &str, value: &str, overwrite: &str) -> String {
    let warehouse = TempDir::new().expect("warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    for statement in seed {
        run(&ctx, &catalogs, statement).await;
    }
    set_session_conf(&ctx, key, value);
    let outcome = execute(&ctx, &catalogs, overwrite).await.map(|_| ());
    unset_session_conf(&ctx, key);
    outcome
        .expect_err("the session property collides with a key the overwrite produces")
        .strip_backtrace()
}

fn merge_on_read_seed(table_name: &str) -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE ice.sales.{table_name} (id BIGINT, data STRING, cat STRING) USING \
             iceberg PARTITIONED BY (cat) TBLPROPERTIES ('format-version' = '2', \
             'write.delete.mode' = 'merge-on-read')"
        ),
        format!("INSERT INTO ice.sales.{table_name} VALUES (1,'a','x'),(2,'b','x'),(3,'c','y')"),
        format!("DELETE FROM ice.sales.{table_name} WHERE id = 1"),
    ]
}

async fn merge_on_read_refusal(table_name: &str, key: &str, value: &str) -> String {
    overwrite_refusal(
        &merge_on_read_seed(table_name),
        &format!("spark.sql.iceberg.snapshot-property.{key}"),
        value,
        &format!("INSERT OVERWRITE ice.sales.{table_name} PARTITION (cat = 'x') VALUES (9,'z')"),
    )
    .await
}

#[tokio::test]
async fn a_merge_on_read_overwrite_names_the_delete_file_it_removes() {
    let _: &str = "pins: ice-session-write-conf-1/C-059";
    let error = merge_on_read_refusal("mordel", "removed-delete-files", "9").await;
    assert_eq!(
        error,
        "External error: Multiple entries with same key: removed-delete-files=1 and \
         removed-delete-files=9",
        "the removed set must carry the position-delete file the overwrite drops"
    );
}

#[tokio::test]
async fn a_merge_on_read_overwrite_names_the_positions_it_removes() {
    let _: &str = "pins: ice-session-write-conf-1/C-059";
    let error = merge_on_read_refusal("morpos", "removed-position-deletes", "9").await;
    assert_eq!(
        error,
        "External error: Multiple entries with same key: removed-position-deletes=1 and \
         removed-position-deletes=9",
        "the delete file's record count is the positions the overwrite removes"
    );
}

#[tokio::test]
async fn a_merge_on_read_overwrite_subtracts_the_delete_file_from_the_total() {
    let _: &str = "pins: ice-session-write-conf-1/C-059";
    let error = merge_on_read_refusal("mortot", "total-delete-files", "1").await;
    assert_eq!(
        error,
        "External error: Multiple entries with same key: total-delete-files=0 and \
         total-delete-files=1",
        "one live delete file minus the one removed is zero, not the untouched one"
    );
}

#[tokio::test]
async fn a_merge_on_read_overwrite_names_the_records_of_the_data_file_it_removes() {
    let _: &str = "pins: ice-session-write-conf-1/C-059";
    let error = merge_on_read_refusal("mordr", "deleted-records", "5").await;
    assert_eq!(
        error,
        "External error: Multiple entries with same key: deleted-records=2 and \
         deleted-records=5",
        "the data file still holds both rows; the position delete does not lower its record count"
    );
}
