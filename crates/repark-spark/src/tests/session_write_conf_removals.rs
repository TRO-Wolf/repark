use super::super::*;

const DELETED_RECORDS: &str = "spark.sql.iceberg.snapshot-property.deleted-records";
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

async fn identity_partition_refusal(
    table_name: &str,
    column_type: &str,
    values: &str,
    literal: &str,
) -> String {
    let seed = vec![
        format!(
            "CREATE TABLE ice.sales.{table_name} (id BIGINT, k {column_type}) USING iceberg \
             PARTITIONED BY (k)"
        ),
        format!("INSERT INTO ice.sales.{table_name} VALUES {values}"),
    ];
    overwrite_refusal(
        &seed,
        DELETED_RECORDS,
        "5",
        &format!("INSERT OVERWRITE ice.sales.{table_name} PARTITION (k = '{literal}') VALUES (9)"),
    )
    .await
}

fn names_two_live_rows(error: &str, column_type: &str) {
    assert_eq!(
        error,
        "External error: Multiple entries with same key: deleted-records=2 and \
         deleted-records=5",
        "the {column_type} equality must resolve the partition it clears"
    );
}

#[tokio::test]
async fn an_identity_decimal_partition_overwrite_names_the_engine_value() {
    let _: &str = "pins: ice-session-write-conf-1/C-060";
    let error = identity_partition_refusal(
        "sodec",
        "DECIMAL(10,2)",
        "(1, 1.50),(2, 1.50),(3, 2.00)",
        "1.50",
    )
    .await;
    names_two_live_rows(&error, "DECIMAL(10,2)");
}

#[tokio::test]
async fn an_identity_double_partition_overwrite_names_the_engine_value() {
    let _: &str = "pins: ice-session-write-conf-1/C-060";
    let error =
        identity_partition_refusal("sodbl", "DOUBLE", "(1, 1.5),(2, 1.5),(3, 2.0)", "1.5").await;
    names_two_live_rows(&error, "DOUBLE");
}

#[tokio::test]
async fn an_identity_boolean_partition_overwrite_names_the_engine_value() {
    let _: &str = "pins: ice-session-write-conf-1/C-060";
    let error = identity_partition_refusal(
        "sobool",
        "BOOLEAN",
        "(1, true),(2, true),(3, false)",
        "true",
    )
    .await;
    names_two_live_rows(&error, "BOOLEAN");
}

#[tokio::test]
async fn an_empty_source_static_overwrite_names_the_partition_it_clears() {
    let _: &str = "pins: ice-session-write-conf-1/C-061";
    let seed = vec![
        "CREATE TABLE ice.sales.soempty (id BIGINT, cat STRING) USING iceberg PARTITIONED BY \
         (cat)"
            .to_string(),
        "INSERT INTO ice.sales.soempty VALUES (1,'x'),(2,'x'),(3,'y')".to_string(),
    ];
    let error = overwrite_refusal(
        &seed,
        DELETED_RECORDS,
        "5",
        "INSERT OVERWRITE ice.sales.soempty PARTITION (cat = 'x') \
         SELECT id FROM ice.sales.soempty WHERE id > 100",
    )
    .await;
    assert_eq!(
        error,
        "External error: Multiple entries with same key: deleted-records=2 and \
         deleted-records=5",
        "an empty source stages no file, and the partition equalities are the only thing \
         that can name what the overwrite clears"
    );
}
