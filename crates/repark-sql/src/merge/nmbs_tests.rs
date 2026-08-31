//! pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-004, C-005, C-006
//! ANSI-door `WHEN NOT MATCHED BY SOURCE` execute pins.

use datafusion::arrow::datatypes::DataType;

use super::cardinality_tests::{door_with_schema, id_name_rows};

const COW: &str = "extra_properties = MAP(\
     ARRAY['write.merge.mode'], ARRAY['copy-on-write'])";

const MOR: &str = "extra_properties = MAP(\
     ARRAY['write.merge.mode', 'write.delete.mode', 'write.update.mode'], \
     ARRAY['merge-on-read', 'merge-on-read', 'merge-on-read'])";

#[tokio::test]
async fn ansi_cow_and_mor_nmbs_delete_keeps_matched() {
    for (name, props) in [("nmbs_del_cow", COW), ("nmbs_del_mor", MOR)] {
        let door = door_with_schema().await;
        door.ok(&format!(
            "CREATE TABLE ice.sales.{name} (id BIGINT, name VARCHAR) WITH ({props})"
        ))
        .await;
        door.ok(&format!(
            "INSERT INTO ice.sales.{name} VALUES (1, 'a'), (2, 'b'), (3, 'c')"
        ))
        .await;
        door.ok(&format!(
            "CREATE TABLE ice.sales.{name}_src AS SELECT CAST(1 AS BIGINT) AS id, 'aa' AS name"
        ))
        .await;
        door.ok(&format!(
            "MERGE INTO ice.sales.{name} AS t USING ice.sales.{name}_src AS s ON t.id = s.id \
             WHEN NOT MATCHED BY SOURCE THEN DELETE"
        ))
        .await;
        let (schema, batches) = door
            .ok_typed(&format!(
                "SELECT id, name FROM ice.sales.{name} ORDER BY id"
            ))
            .await;
        assert_eq!(schema.field(0).data_type(), &DataType::Int64, "{name} id");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "{name} name");
        assert_eq!(id_name_rows(&batches), vec![(1, "a".to_string())], "{name}");
    }
}

#[tokio::test]
async fn ansi_nmbs_update_and_three_arms() {
    let door = door_with_schema().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_upd (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.nmbs_upd VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("CREATE TABLE ice.sales.nmbs_upd_src AS SELECT CAST(1 AS BIGINT) AS id, 'aa' AS name")
        .await;
    door.ok(
        "MERGE INTO ice.sales.nmbs_upd AS t USING ice.sales.nmbs_upd_src AS s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET name = 'gone'",
    )
    .await;
    let (schema, batches) = door
        .ok_typed("SELECT id, name FROM ice.sales.nmbs_upd ORDER BY id")
        .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "upd id");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "upd name");
    assert_eq!(
        id_name_rows(&batches),
        vec![(1, "a".to_string()), (2, "gone".to_string())]
    );

    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_3 (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.nmbs_3 VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("CREATE TABLE ice.sales.nmbs_3_src AS \
         SELECT CAST(1 AS BIGINT) AS id, 'aa' AS name \
         UNION ALL SELECT CAST(4 AS BIGINT), 'dd'")
        .await;
    door.ok(
        "MERGE INTO ice.sales.nmbs_3 AS t USING ice.sales.nmbs_3_src AS s ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET name = s.name \
         WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name) \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    let (schema, batches) = door
        .ok_typed("SELECT id, name FROM ice.sales.nmbs_3 ORDER BY id")
        .await;
    assert_eq!(
        schema.field(0).data_type(),
        &DataType::Int64,
        "three-arm id"
    );
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Utf8,
        "three-arm name"
    );
    assert_eq!(
        id_name_rows(&batches),
        vec![(1, "aa".to_string()), (4, "dd".to_string())]
    );
}

#[tokio::test]
async fn ansi_source_empty_wipe_and_cardinality() {
    let door = door_with_schema().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_wipe (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.nmbs_wipe VALUES (1, 'a'), (2, 'b'), (3, 'c')")
        .await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_wipe_src (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok(
        "MERGE INTO ice.sales.nmbs_wipe AS t USING ice.sales.nmbs_wipe_src AS s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    let (schema, batches) = door
        .ok_typed("SELECT id, name FROM ice.sales.nmbs_wipe ORDER BY id")
        .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64, "wipe id");
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8, "wipe name");
    assert!(id_name_rows(&batches).is_empty());

    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_card (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.nmbs_card VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("CREATE TABLE ice.sales.nmbs_card_src AS \
         SELECT CAST(1 AS BIGINT) AS id, 'x' AS name \
         UNION ALL SELECT CAST(1 AS BIGINT), 'y'")
        .await;
    let err = door
        .err(
            "MERGE INTO ice.sales.nmbs_card AS t USING ice.sales.nmbs_card_src AS s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )
        .await;
    assert!(err.contains("MERGE_CARDINALITY_VIOLATION"), "{err}");
}

#[tokio::test]
async fn ansi_matched_predicate_miss_is_not_nmbs() {
    let door = door_with_schema().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_miss (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.nmbs_miss VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok("CREATE TABLE ice.sales.nmbs_miss_src AS SELECT CAST(1 AS BIGINT) AS id, 'aa' AS name")
        .await;
    door.ok(
        "MERGE INTO ice.sales.nmbs_miss AS t USING ice.sales.nmbs_miss_src AS s ON t.id = s.id \
         WHEN MATCHED AND t.name = 'NOPE' THEN UPDATE SET name = s.name \
         WHEN NOT MATCHED BY SOURCE THEN DELETE",
    )
    .await;
    let (schema, batches) = door
        .ok_typed("SELECT id, name FROM ice.sales.nmbs_miss ORDER BY id")
        .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(id_name_rows(&batches), vec![(1, "a".to_string())]);
}

#[tokio::test]
async fn ansi_source_empty_nmbs_update() {
    let door = door_with_schema().await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_eu (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok("INSERT INTO ice.sales.nmbs_eu VALUES (1, 'a'), (2, 'b')")
        .await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.nmbs_eu_src (id BIGINT, name VARCHAR) WITH ({COW})"
    ))
    .await;
    door.ok(
        "MERGE INTO ice.sales.nmbs_eu AS t USING ice.sales.nmbs_eu_src AS s ON t.id = s.id \
         WHEN NOT MATCHED BY SOURCE THEN UPDATE SET name = 'x'",
    )
    .await;
    let (schema, batches) = door
        .ok_typed("SELECT id, name FROM ice.sales.nmbs_eu ORDER BY id")
        .await;
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert_eq!(
        id_name_rows(&batches),
        vec![(1, "x".to_string()), (2, "x".to_string())]
    );
}
