use crate::normalize::parse_single_normalized;
use crate::write_door::{WriteDoor, door_of};
use crate::write_options::StatementWriteOptions;

const EVALUATION_IDS: [&str; 18] = [
    "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "D10", "D14", "D15", "D16", "D17", "D18",
    "D19", "D20", "D21",
];

fn door(sql: &str, options: &StatementWriteOptions) -> Option<WriteDoor> {
    let (statement, _, _) = parse_single_normalized(sql)
        .expect("the statement parses")
        .expect("one statement");
    door_of(&statement, options)
}

fn with_option() -> StatementWriteOptions {
    StatementWriteOptions {
        raw: vec![("write.format.default".to_string(), "parquet".to_string())],
        ..StatementWriteOptions::default()
    }
}

#[test]
fn write_door_ids_are_the_evaluation_table_ids_and_the_first_ten_are_spark_doors() {
    let ids: Vec<&str> = WriteDoor::ALL.iter().map(|door| door.id()).collect();
    assert_eq!(ids, EVALUATION_IDS);
    let spark: Vec<&str> = WriteDoor::ALL
        .iter()
        .filter(|door| door.is_spark_door())
        .map(|door| door.id())
        .collect();
    assert_eq!(spark, EVALUATION_IDS[..10]);
}

#[test]
fn door_of_names_the_door_each_parsed_spark_write_takes() {
    let none = StatementWriteOptions::empty();
    let cases = [
        (
            "INSERT INTO ice.ns.t VALUES (1, 2)",
            &none,
            Some(WriteDoor::D1Insert),
        ),
        (
            "INSERT INTO ice.ns.t SELECT id, v FROM ice.ns.s",
            &none,
            Some(WriteDoor::D1Insert),
        ),
        (
            "INSERT INTO ice.ns.t VALUES (1, 2)",
            &with_option(),
            Some(WriteDoor::D2InsertOptions),
        ),
        (
            "INSERT OVERWRITE ice.ns.t SELECT id, v FROM ice.ns.s",
            &none,
            Some(WriteDoor::D4InsertOverwrite),
        ),
        (
            "INSERT OVERWRITE ice.ns.t VALUES (1, 2)",
            &with_option(),
            Some(WriteDoor::D4InsertOverwrite),
        ),
        (
            "CREATE TABLE ice.ns.c USING iceberg AS SELECT 1 AS id",
            &none,
            Some(WriteDoor::D6Ctas),
        ),
        (
            "CREATE OR REPLACE TABLE ice.ns.c USING iceberg AS SELECT 1 AS id",
            &none,
            Some(WriteDoor::D6Ctas),
        ),
        (
            "MERGE INTO ice.ns.t t USING ice.ns.s s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)",
            &none,
            Some(WriteDoor::D7MergeInsert),
        ),
        (
            "MERGE INTO ice.ns.t t USING ice.ns.s s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.st = s.st",
            &none,
            Some(WriteDoor::D8MergeUpdateStruct),
        ),
        (
            "MERGE INTO ice.ns.t t USING ice.ns.s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *",
            &none,
            Some(WriteDoor::D8MergeUpdateStruct),
        ),
        (
            "MERGE INTO ice.ns.t t USING ice.ns.s s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.st.v = s.v",
            &none,
            Some(WriteDoor::D9MergeUpdateField),
        ),
        (
            "UPDATE ice.ns.t SET st.v = TIMESTAMP '2026-01-02 03:04:05' WHERE id >= 0",
            &none,
            Some(WriteDoor::D10Update),
        ),
    ];
    for (sql, options, expected) in cases {
        assert_eq!(door(sql, options), expected, "{sql}");
    }
}

#[test]
fn door_of_answers_none_for_statements_that_write_no_new_values() {
    let none = StatementWriteOptions::empty();
    for sql in [
        "DELETE FROM ice.ns.t WHERE id = 1",
        "CALL ice.system.rewrite_data_files('ns.t')",
        "TRUNCATE TABLE ice.ns.t",
        "SELECT * FROM ice.ns.t",
        "CREATE TABLE ice.ns.c (id INT) USING iceberg",
        "DROP TABLE ice.ns.c",
        "MERGE INTO ice.ns.t t USING ice.ns.s s ON t.id = s.id WHEN MATCHED THEN DELETE",
    ] {
        assert_eq!(door(sql, &none), None, "{sql}");
    }
}
