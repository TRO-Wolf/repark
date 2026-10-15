use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray};
use datafusion::arrow::datatypes::Int32Type;
use repark_core::{OverwriteIntent, ReparkSession};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const TARGET: [&str; 2] = ["a", "b"];

const SHAPES: [(&str, &[(&str, i32)]); 4] = [
    ("reordered", &[("b", 2), ("a", 1)]),
    ("renamed", &[("a", 1), ("q", 2)]),
    ("extra", &[("a", 1), ("b", 2), ("c", 3)]),
    ("missing", &[("a", 1)]),
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Position,
    Name,
}

#[derive(Clone, Copy)]
enum Door {
    Sql(&'static str),
    Frame(&'static str, OverwriteIntent, bool),
}

const DOORS: [(&str, Mode, Door); 16] = [
    (
        "insert",
        Mode::Position,
        Door::Sql("INSERT INTO {t} SELECT 1, {s}"),
    ),
    (
        "insert_values",
        Mode::Position,
        Door::Sql("INSERT INTO {t} VALUES (1, {s})"),
    ),
    (
        "insert_overwrite",
        Mode::Position,
        Door::Sql("INSERT OVERWRITE {t} SELECT 1, {s}"),
    ),
    (
        "replace_where",
        Mode::Position,
        Door::Sql("INSERT INTO {t} REPLACE WHERE id >= 0 SELECT 1, {s}"),
    ),
    (
        "df_insert_into",
        Mode::Position,
        Door::Frame(
            "INSERT INTO {t} SELECT * FROM (SELECT 1 AS id, {s} AS st) v",
            OverwriteIntent::Session,
            false,
        ),
    ),
    (
        "df_insert_into_overwrite",
        Mode::Position,
        Door::Frame(
            "INSERT OVERWRITE {t} SELECT * FROM (SELECT 1 AS id, {s} AS st) v",
            OverwriteIntent::Session,
            false,
        ),
    ),
    (
        "insert_by_name",
        Mode::Name,
        Door::Sql("INSERT INTO {t} BY NAME SELECT {s} AS st, 1 AS id"),
    ),
    (
        "merge_insert",
        Mode::Name,
        Door::Sql(
            "MERGE INTO {t} t USING (SELECT 1 AS id, {s} AS st) s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)",
        ),
    ),
    (
        "merge_insert_star",
        Mode::Name,
        Door::Sql(
            "MERGE INTO {t} t USING (SELECT 1 AS id, {s} AS st) s ON t.id = s.id \
             WHEN NOT MATCHED THEN INSERT *",
        ),
    ),
    (
        "merge_update",
        Mode::Name,
        Door::Sql(
            "MERGE INTO {t} t USING (SELECT 0 AS id, {s} AS st) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET t.st = s.st",
        ),
    ),
    (
        "merge_update_star",
        Mode::Name,
        Door::Sql(
            "MERGE INTO {t} t USING (SELECT 0 AS id, {s} AS st) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET *",
        ),
    ),
    (
        "update",
        Mode::Name,
        Door::Sql("UPDATE {t} SET st = {s} WHERE id = 0"),
    ),
    (
        "update_no_where",
        Mode::Name,
        Door::Sql("UPDATE {t} SET st = {s}"),
    ),
    (
        "df_append",
        Mode::Name,
        Door::Frame(
            "INSERT INTO {t} (id, st) SELECT id, st FROM (SELECT 1 AS id, {s} AS st) v",
            OverwriteIntent::Session,
            true,
        ),
    ),
    (
        "df_overwrite_partitions",
        Mode::Name,
        Door::Frame(
            "INSERT OVERWRITE {t} (id, st) SELECT id, st FROM (SELECT 1 AS id, {s} AS st) v",
            OverwriteIntent::Dynamic,
            true,
        ),
    ),
    (
        "df_save_append",
        Mode::Name,
        Door::Frame(
            "INSERT INTO {t} (id, st) SELECT id, st FROM (SELECT 1 AS id, {s} AS st) v",
            OverwriteIntent::Session,
            true,
        ),
    ),
];

const UNLABELLED: [&str; 6] = [
    "merge_insert",
    "merge_insert_star",
    "merge_update",
    "merge_update_star",
    "update",
    "update_no_where",
];

const UPDATING: [&str; 4] = [
    "merge_update",
    "merge_update_star",
    "update",
    "update_no_where",
];

const R014: &str = "update_no_where";

const R014_RAW: &str = "arguments need to have the same data type";

const R018_RAW: &str = "type mismatch and can't cast";

#[derive(Debug, PartialEq, Eq)]
enum Expected {
    Stored(Option<i32>, Option<i32>),
    Refused(String),
}

fn rule(mode: Mode, shape: &[(&str, i32)], label: &str) -> Expected {
    let refuse = |class: &str, detail: String| {
        Expected::Refused(format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.{class}] Cannot write incompatible data for the table \
             {label}: {detail}. SQLSTATE: KD000"
        ))
    };
    if mode == Mode::Position {
        if shape.len() > TARGET.len() {
            return refuse(
                "EXTRA_STRUCT_FIELDS",
                format!(
                    "Cannot write extra fields `{}` to the struct `st`",
                    shape[2].0
                ),
            );
        }
        if shape.len() < TARGET.len() {
            return refuse(
                "STRUCT_MISSING_FIELDS",
                "Struct `st` missing fields: `b`".to_string(),
            );
        }
        return Expected::Stored(Some(shape[0].1), Some(shape[1].1));
    }
    let mut values = Vec::new();
    for target in TARGET {
        let Some((_, value)) = shape
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(target))
        else {
            return refuse(
                "CANNOT_FIND_DATA",
                format!("Cannot find data for the output column `st`.`{target}`"),
            );
        };
        values.push(*value);
    }
    if shape.len() > TARGET.len() {
        return refuse(
            "EXTRA_STRUCT_FIELDS",
            format!(
                "Cannot write extra fields `{}` to the struct `st`",
                shape[2].0
            ),
        );
    }
    Expected::Stored(Some(values[0]), Some(values[1]))
}

fn session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .build()
        .expect("session")
}

async fn attempt(
    session: &ReparkSession,
    door: Door,
    table: &str,
    source: &str,
) -> Result<(), String> {
    let (sql, frame) = match door {
        Door::Sql(template) => (template, None),
        Door::Frame(template, intent, by_name) => (template, Some((intent, by_name))),
    };
    let sql = sql.replace("{t}", table).replace("{s}", source);
    let planned = match frame {
        None => session.sql(&sql).await,
        Some((intent, by_name)) => {
            session
                .sql_built_with_write_options(&sql, &HashMap::new(), intent, by_name)
                .await
        }
    };
    match planned {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn run(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .expect("plans")
        .collect()
        .await
        .expect("runs");
}

async fn stored(
    session: &ReparkSession,
    table: &str,
    id: i32,
) -> Option<(Option<i32>, Option<i32>)> {
    let batches = session
        .sql(&format!("SELECT st.a, st.b FROM {table} WHERE id = {id}"))
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let batch = batches.iter().find(|batch| batch.num_rows() > 0)?;
    let value = |index: usize| {
        let column = batch.column(index).as_primitive::<Int32Type>();
        (!column.is_null(0)).then(|| column.value(0))
    };
    Some((value(0), value(1)))
}

#[tokio::test]
async fn every_door_pairs_struct_fields_by_spark_rule() {
    let session = session();
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    let mut failures = Vec::new();
    for (name, mode, door) in DOORS {
        for (shape, fields) in SHAPES {
            let table = format!("ice.ns.{name}_{shape}");
            let create =
                format!("CREATE TABLE {table} (id INT, st STRUCT<a: INT, b: INT>) USING iceberg");
            run(&session, &create).await;
            let seed = format!("INSERT INTO {table} VALUES (0, named_struct('a', 10, 'b', 20))");
            run(&session, &seed).await;
            let source = format!(
                "named_struct({})",
                fields
                    .iter()
                    .map(|(field, value)| format!("'{field}', {value}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let label = if UNLABELLED.contains(&name) {
                "``".to_string()
            } else {
                format!("`ice`.`ns`.`{name}_{shape}`")
            };
            let written = i32::from(!UPDATING.contains(&name));
            let expected = if name == R014 && shape == "reordered" {
                Expected::Refused(R014_RAW.to_string())
            } else if name == "insert_values" && fields.len() != TARGET.len() {
                Expected::Refused(R018_RAW.to_string())
            } else {
                rule(mode, fields, &label)
            };
            let outcome = attempt(&session, door, &table, &source).await;
            let answered = match outcome {
                Ok(()) => stored(&session, &table, written).await.map_or_else(
                    || Expected::Refused("no row".to_string()),
                    |(a, b)| Expected::Stored(a, b),
                ),
                Err(text) => Expected::Refused(text),
            };
            let holds = match (&expected, &answered) {
                (Expected::Refused(want), Expected::Refused(got)) => got.contains(want.as_str()),
                (want, got) => want == got,
            };
            if !holds {
                failures.push(format!(
                    "{name}/{shape}: expected {expected:?}, answered {answered:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} cells:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

const HINT_H1_PARTITION: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write \
     incompatible data for the table ``: the struct `st` has the fields `b`, `a` and the table \
     has `a`, `b`. INSERT OVERWRITE with a static PARTITION value does not pair struct fields; \
     write the struct with the table's field names in the table's field order.";

const HINT_H1_BATCH: &str = "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write \
     incompatible data for the table ``: the struct `st` has the fields `b`, `a` and the table \
     has `a`, `b`. This entry does not pair struct fields; give the value the table's field \
     names in the table's order.";

#[tokio::test]
async fn the_partition_overwrite_route_refuses_an_unpaired_struct_with_hint_h1() {
    let session = session();
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    let table = "ice.ns.static_part";
    run(
        &session,
        &format!(
            "CREATE TABLE {table} (id INT, p INT, st STRUCT<a: INT, b: INT>) USING iceberg \
             PARTITIONED BY (p)"
        ),
    )
    .await;
    run(
        &session,
        &format!("INSERT INTO {table} VALUES (0, 1, named_struct('a', 10, 'b', 20))"),
    )
    .await;
    for (statement, hint) in [
        (
            format!(
                "INSERT OVERWRITE {table} PARTITION (p = 1) SELECT 1, named_struct('b', 2, 'a', 1)"
            ),
            HINT_H1_PARTITION,
        ),
        (
            format!(
                "INSERT OVERWRITE {table} PARTITION (p) SELECT 1, 1, named_struct('b', 2, 'a', 1)"
            ),
            HINT_H1_BATCH,
        ),
    ] {
        let refusal = attempt(&session, Door::Sql("{t}"), &statement, "")
            .await
            .expect_err("the partition route refuses an unpaired struct");
        assert!(refusal.contains(hint), "{statement}: {refusal}");
        assert_eq!(stored(&session, table, 0).await, Some((Some(10), Some(20))));
    }
    run(
        &session,
        &format!(
            "INSERT OVERWRITE {table} PARTITION (p = 1) SELECT 1, named_struct('a', 1, 'b', 2)"
        ),
    )
    .await;
    assert_eq!(stored(&session, table, 1).await, Some((Some(1), Some(2))));
    assert_eq!(stored(&session, table, 0).await, None);
}

fn set_case_sensitive(session: &ReparkSession, enabled: bool) {
    session
        .context()
        .state_ref()
        .write()
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_functions::case_sensitive::SparkCaseSensitiveConfig { enabled });
}

#[tokio::test]
async fn a_by_name_frame_door_pairs_under_the_session_case_rule() {
    let session = session();
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    let door = Door::Frame(
        "INSERT INTO {t} (id, st) SELECT id, st FROM (SELECT 1 AS id, {s} AS st) v",
        OverwriteIntent::Session,
        true,
    );
    for (case_sensitive, table) in [
        (false, "ice.ns.recased_insensitive"),
        (true, "ice.ns.recased_sensitive"),
    ] {
        run(
            &session,
            &format!("CREATE TABLE {table} (id INT, st STRUCT<a: INT, b: INT>) USING iceberg"),
        )
        .await;
        set_case_sensitive(&session, case_sensitive);
        let outcome = attempt(&session, door, table, "named_struct('B', 2, 'A', 1)").await;
        set_case_sensitive(&session, false);
        if case_sensitive {
            let refusal = outcome.expect_err("a recased field under caseSensitive=true");
            assert!(
                refusal.contains(
                    "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible \
                     data for the table `ice`.`ns`.`recased_sensitive`: Cannot find data for the \
                     output column `st`.`a`."
                ),
                "{refusal}"
            );
            assert_eq!(stored(&session, table, 1).await, None);
        } else {
            outcome.expect("a recased field pairs by name under caseSensitive=false");
            assert_eq!(stored(&session, table, 1).await, Some((Some(1), Some(2))));
        }
    }
}

fn inline_table_refusal(column: usize) -> String {
    format!(
        "[INVALID_INLINE_TABLE.INCOMPATIBLE_TYPES_IN_INLINE_TABLE] Invalid inline table. Found \
         incompatible types in the column `col{column}` for inline table. SQLSTATE: 42000"
    )
}

const VALUES_ROWS_CASES: [(&str, &str, bool, Option<usize>); 9] = [
    (
        "rows_null",
        "INSERT INTO {t} VALUES (1, named_struct('b', 2, 'a', 1)), (2, NULL)",
        false,
        None,
    ),
    (
        "rows_nocol",
        "INSERT INTO {t} VALUES (1, named_struct('b', 2, 'a', 1)), \
         (2, named_struct('x', 7, 'y', 8))",
        false,
        Some(2),
    ),
    (
        "rows_in_order",
        "INSERT INTO {t} (id, st) VALUES (1, named_struct('b', 2, 'a', 1)), \
         (2, named_struct('x', 7, 'y', 8))",
        false,
        Some(2),
    ),
    (
        "rows_swapped",
        "INSERT INTO {t} (st, id) VALUES (named_struct('b', 2, 'a', 1), 1), \
         (named_struct('x', 7, 'y', 8), 2)",
        false,
        Some(1),
    ),
    (
        "rows_order",
        "INSERT INTO {t} VALUES (1, named_struct('b', 2, 'a', 1)), \
         (2, named_struct('a', 7, 'b', 8))",
        false,
        Some(2),
    ),
    (
        "rows_case_sensitive",
        "INSERT INTO {t} VALUES (1, named_struct('B', 2, 'a', 1)), \
         (2, named_struct('b', 8, 'a', 7))",
        true,
        Some(2),
    ),
    (
        "rows_case_insensitive",
        "INSERT INTO {t} VALUES (1, named_struct('B', 2, 'a', 1)), \
         (2, named_struct('b', 8, 'a', 7))",
        false,
        None,
    ),
    (
        "rows_agree",
        "INSERT INTO {t} VALUES (1, named_struct('b', 2, 'a', 1)), \
         (2, named_struct('b', 8, 'a', 7))",
        false,
        None,
    ),
    (
        "rows_agree_swapped",
        "INSERT INTO {t} (st, id) VALUES (named_struct('b', 2, 'a', 1), 1), \
         (named_struct('b', 8, 'a', 7), 2)",
        false,
        None,
    ),
];

#[tokio::test]
async fn multi_row_values_refuse_as_spark_when_rows_disagree_on_struct_names() {
    let session = session();
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    for (name, template, case_sensitive, refused_column) in VALUES_ROWS_CASES {
        let table = format!("ice.ns.{name}");
        run(
            &session,
            &format!("CREATE TABLE {table} (id INT, st STRUCT<a: INT, b: INT>) USING iceberg"),
        )
        .await;
        run(
            &session,
            &format!("INSERT INTO {table} VALUES (0, named_struct('a', 10, 'b', 20))"),
        )
        .await;
        set_case_sensitive(&session, case_sensitive);
        let outcome = attempt(&session, Door::Sql(template), &table, "").await;
        set_case_sensitive(&session, false);
        if let Some(column) = refused_column {
            let refusal = outcome.expect_err("rows that disagree on struct names refuse");
            assert!(
                refusal.contains(&inline_table_refusal(column)),
                "{name}: {refusal}"
            );
            assert_eq!(stored(&session, &table, 1).await, None, "{name}");
            assert_eq!(stored(&session, &table, 2).await, None, "{name}");
        } else {
            outcome.unwrap_or_else(|error| panic!("{name}: {error}"));
            assert_eq!(
                stored(&session, &table, 1).await,
                Some((Some(2), Some(1))),
                "{name}"
            );
            let second = if name == "rows_null" {
                (None, None)
            } else {
                (Some(8), Some(7))
            };
            assert_eq!(stored(&session, &table, 2).await, Some(second), "{name}");
        }
        assert_eq!(
            stored(&session, &table, 0).await,
            Some((Some(10), Some(20))),
            "{name}"
        );
    }
}

async fn seeded_table(session: &ReparkSession, table: &str, column: &str, seed: &str) {
    run(
        session,
        &format!("CREATE TABLE {table} (id INT, st {column}) USING iceberg"),
    )
    .await;
    run(session, &format!("INSERT INTO {table} VALUES (0, {seed})")).await;
}

async fn catalog_session() -> (ReparkSession, TempDir) {
    let session = session();
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(&session, "CREATE NAMESPACE ice.ns").await;
    (session, warehouse)
}

#[tokio::test]
async fn a_paired_struct_leaf_spark_refuses_refuses_on_every_insert_spelling() {
    let (session, _warehouse) = catalog_session().await;
    for (name, template) in [
        (
            "leaf_select",
            "INSERT INTO {t} SELECT 1, named_struct('b', '5', 'a', 1)",
        ),
        (
            "leaf_values",
            "INSERT INTO {t} VALUES (1, named_struct('b', '5', 'a', 1))",
        ),
        (
            "leaf_collist_values",
            "INSERT INTO {t} (st, id) VALUES (named_struct('b', '5', 'a', 1), 1)",
        ),
        (
            "leaf_overwrite",
            "INSERT OVERWRITE {t} SELECT 1, named_struct('b', '5', 'a', 1)",
        ),
    ] {
        let table = format!("ice.ns.{name}");
        seeded_table(
            &session,
            &table,
            "STRUCT<a: BIGINT, b: STRING>",
            "named_struct('a', CAST(10 AS BIGINT), 'b', 'x')",
        )
        .await;
        let refusal = attempt(&session, Door::Sql(template), &table, "")
            .await
            .expect_err(name);
        let expected = format!(
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data for \
             the table `ice`.`ns`.`{name}`: Cannot safely cast `st`.`a` \"STRING\" to \
             \"BIGINT\". SQLSTATE: KD000"
        );
        assert!(refusal.contains(&expected), "{name}: {refusal}");
        let rows = session
            .sql(&format!("SELECT id FROM {table}"))
            .await
            .expect("read")
            .collect()
            .await
            .expect("collect");
        let count: usize = rows.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(
            count, 1,
            "{name}: the seed row stays and nothing is written"
        );
    }
}

#[tokio::test]
async fn a_column_list_values_row_maps_each_value_to_its_listed_column() {
    let (session, _warehouse) = catalog_session().await;
    for (name, template, expected) in [
        (
            "collist_swapped",
            "INSERT INTO {t} (st, id) VALUES (named_struct('b', 2, 'a', 1), 1)",
            (Some(2), Some(1)),
        ),
        (
            "collist_in_order",
            "INSERT INTO {t} (id, st) VALUES (1, named_struct('b', 2, 'a', 1))",
            (Some(2), Some(1)),
        ),
        (
            "collist_swapped_table_order",
            "INSERT INTO {t} (st, id) VALUES (named_struct('a', 3, 'b', 4), 1)",
            (Some(3), Some(4)),
        ),
    ] {
        let table = format!("ice.ns.{name}");
        seeded_table(
            &session,
            &table,
            "STRUCT<a: INT, b: INT>",
            "named_struct('a', 10, 'b', 20)",
        )
        .await;
        attempt(&session, Door::Sql(template), &table, "")
            .await
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(stored(&session, &table, 1).await, Some(expected), "{name}");
    }
}

#[tokio::test]
async fn the_update_refusal_pairs_under_the_session_case_rule() {
    let (session, _warehouse) = catalog_session().await;
    for (name, case_sensitive, where_sql, expected) in [
        (
            "update_twins_case_sensitive",
            true,
            " WHERE id = 0",
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for \
             the table ``: Cannot find data for the output column `st`.`b`. SQLSTATE: KD000",
        ),
        (
            "update_no_where_twins_case_sensitive",
            true,
            "",
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for \
             the table ``: Cannot find data for the output column `st`.`b`. SQLSTATE: KD000",
        ),
    ] {
        let table = format!("ice.ns.{name}");
        seeded_table(
            &session,
            &table,
            "STRUCT<a: INT, b: INT>",
            "named_struct('a', 10, 'b', 20)",
        )
        .await;
        set_case_sensitive(&session, case_sensitive);
        let outcome = attempt(
            &session,
            Door::Sql("UPDATE {t} SET st = named_struct('a', 1, 'A', 2){s}"),
            &table,
            where_sql,
        )
        .await;
        set_case_sensitive(&session, false);
        let refusal = outcome.expect_err(name);
        assert!(refusal.contains(expected), "{name}: {refusal}");
        assert_eq!(
            stored(&session, &table, 0).await,
            Some((Some(10), Some(20))),
            "{name}"
        );
    }
}

#[tokio::test]
async fn an_update_from_a_column_subquery_with_a_comparison_stores_spark_value() {
    let (session, _warehouse) = catalog_session().await;
    for (name, source_type, source_value) in [
        (
            "subquery_reordered",
            "STRUCT<b: INT, a: INT>",
            "named_struct('b', 2, 'a', 1)",
        ),
        (
            "subquery_in_order",
            "STRUCT<a: INT, b: INT>",
            "named_struct('a', 1, 'b', 2)",
        ),
    ] {
        let table = format!("ice.ns.{name}");
        seeded_table(
            &session,
            &table,
            "STRUCT<a: INT, b: INT>",
            "named_struct('a', 10, 'b', 20)",
        )
        .await;
        seeded_table(&session, &format!("{table}_src"), source_type, source_value).await;
        attempt(
            &session,
            Door::Sql("UPDATE {t} SET st = (SELECT st FROM {t}_src) WHERE id = 0"),
            &table,
            "",
        )
        .await
        .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            stored(&session, &table, 0).await,
            Some((Some(1), Some(2))),
            "{name}"
        );
    }
}
