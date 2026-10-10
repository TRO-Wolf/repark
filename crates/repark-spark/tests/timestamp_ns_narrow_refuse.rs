use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const ZONES: [&str; 5] = [
    "UTC",
    "America/New_York",
    "Asia/Kolkata",
    "Asia/Kathmandu",
    "Australia/Lord_Howe",
];
const MOMENTS: [(&str, i64); 5] = [
    ("2026-01-02 03:04:05.123456789", 1_767_323_045_123_456_789),
    ("2026-03-08 02:30:00.000000001", 1_772_937_000_000_000_001),
    ("1969-12-31 23:59:59.999999999", -1),
    ("1969-12-31 23:59:58.999999999", -1_000_000_001),
    ("1969-12-31 23:59:59.000000001", -999_999_999),
];
const SOURCES: [(&str, &str); 2] = [("ns", "timestamp_ns"), ("tzns", "timestamptz_ns")];
const TARGETS: [(&str, &str); 2] = [
    ("timestamp_ns", "TIMESTAMP_NS"),
    ("timestamptz_ns", "TIMESTAMPTZ_NS"),
];
const NARROWED: [&str; 12] = [
    "coalesce({c}, NULL)",
    "CASE WHEN {i} > 0 THEN {c} ELSE NULL END",
    "if({i} > 0, {c}, NULL)",
    "array({c}, NULL)[0]",
    "CASE WHEN {i} < 0 THEN NULL ELSE {c} END",
    "coalesce(NULL, {c})",
    "element_at(array(NULL, {c}), 2)",
    "transform(array({c}), x -> coalesce(x, NULL))[0]",
    "greatest({c}, NULL)",
    "nullif({c}, NULL)",
    "named_struct('f', coalesce({c}, NULL)).f",
    "CASE WHEN {i} = 1 THEN {c} WHEN {i} = 2 THEN TIMESTAMP '2026-01-02 03:04:05' ELSE NULL END",
];
const NARROWED_UNDER_A_WRITTEN_CALL: [&str; 2] = [
    "CAST(coalesce({c}, NULL) AS {n})",
    "CASE WHEN {i} > 0 THEN coalesce({c}, NULL) ELSE CAST(NULL AS {n}) END",
];
const NARROWED_BESIDE_A_VALUE: [&str; 7] = [
    "coalesce({c}, CAST(NULL AS TIMESTAMP))",
    "coalesce({c}, CAST(NULL AS TIMESTAMP_NTZ))",
    "CASE WHEN {i} = 1 THEN {c} ELSE TIMESTAMP '2026-01-02 03:04:05' END",
    "coalesce({c}, CAST({c} AS TIMESTAMP))",
    "greatest({c}, TIMESTAMP '2026-01-02 03:04:05')",
    "array({c}, TIMESTAMP '2026-01-02 03:04:05')[0]",
    "if({i} > 1, {c}, TIMESTAMP_NTZ '2026-01-02 03:04:05')",
];
const WRITTEN: [&str; 8] = [
    "from_utc_timestamp({c}, 'UTC')",
    "CAST({c} AS TIMESTAMP)",
    "TRY_CAST({c} AS TIMESTAMP)",
    "CAST({c} AS TIMESTAMP_NTZ)",
    "date_trunc('second', {c})",
    "CASE WHEN {i} > 0 THEN CAST({c} AS TIMESTAMP) ELSE NULL END",
    "coalesce(CAST({c} AS TIMESTAMP), NULL)",
    "if({i} > 0, date_trunc('second', {c}), NULL)",
];
const UNITS: [&str; 10] = [
    "microsecond",
    "millisecond",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
];
const KEPT: [&str; 7] = [
    "named_struct('f', {c}, 'g', NULL).f",
    "transform(array({c}), x -> x)[0]",
    "{c}",
    "coalesce({c}, CAST(NULL AS {n}))",
    "CASE WHEN {i} > 0 THEN {c} ELSE CAST(NULL AS {n}) END",
    "CASE WHEN {i} > 0 THEN {c} END",
    "CASE WHEN coalesce({c}, NULL) IS NOT NULL THEN {c} ELSE CAST(NULL AS {n}) END",
];
const MOR: &str = ", 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', \
                   'write.merge.mode'='merge-on-read'";
const DOORS: [(&str, &str, &str); 40] = [
    (
        "insert beside a higher-order function",
        "",
        "INSERT INTO {t} (k, v, id) SELECT size(transform(array(id), x -> x)), {e}, id FROM \
         ice.ns.src",
    ),
    (
        "merge, matched update of every column",
        "",
        "MERGE INTO {t} t USING (SELECT id, {e} AS v, 0 AS k, {null} AS c FROM ice.ns.src) s ON \
         t.id = s.id WHEN MATCHED THEN UPDATE SET *",
    ),
    (
        "merge, not matched insert of every column",
        "",
        "MERGE INTO {t} t USING (SELECT id + 10 AS id, {e} AS v, 0 AS k, {null} AS c FROM \
         ice.ns.src) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
    ),
    (
        "update from a scalar subquery",
        "",
        "UPDATE {t} SET v = (SELECT max({ei}) FROM ice.ns.src i) WHERE id >= 0",
    ),
    (
        "insert with the columns reordered",
        "",
        "INSERT INTO {t} (k, v, id) SELECT 0, {e}, id FROM ice.ns.src",
    ),
    (
        "insert of a sorted and limited query",
        "",
        "INSERT INTO {t} (id, v, k) SELECT id, {e}, 0 FROM ice.ns.src ORDER BY id LIMIT 2",
    ),
    (
        "insert values",
        "",
        "INSERT INTO {t} (id, v, k) VALUES (9, {literal}, 0)",
    ),
    (
        "insert select",
        "",
        "INSERT INTO {t} (id, v, k) SELECT id, {e}, 0 FROM ice.ns.src",
    ),
    (
        "insert by position",
        "",
        "INSERT INTO {t} SELECT id, {e}, 0, {null} FROM ice.ns.src",
    ),
    (
        "insert by name",
        "",
        "INSERT INTO {t} BY NAME SELECT 0 AS k, {e} AS v, id FROM ice.ns.src",
    ),
    (
        "insert overwrite",
        "",
        "INSERT OVERWRITE {t} SELECT id, {e} AS v, 0 AS k, {null} AS c FROM ice.ns.src",
    ),
    (
        "insert overwrite, dynamic partition",
        "P",
        "INSERT OVERWRITE {t} SELECT id, {e} AS v, 0 AS k, {null} AS c FROM ice.ns.src",
    ),
    (
        "insert overwrite, static partition",
        "P",
        "INSERT OVERWRITE {t} PARTITION (k = 0) SELECT id, {e} AS v, {null} AS c FROM ice.ns.src",
    ),
    (
        "replace where",
        "",
        "INSERT INTO {t} REPLACE WHERE id >= 0 SELECT id, {e} AS v, 0 AS k, {null} AS c FROM \
         ice.ns.src",
    ),
    (
        "merge, not matched insert",
        "",
        "MERGE INTO {t} t USING (SELECT id + 10 AS id, {source} FROM ice.ns.src) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {es}, 0)",
    ),
    (
        "merge, not matched insert of a subquery column",
        "",
        "MERGE INTO {t} t USING (SELECT id + 10 AS id, {e} AS v FROM ice.ns.src) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, s.v, 0)",
    ),
    (
        "merge, matched update",
        "",
        "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = {es}",
    ),
    (
        "merge, matched update of a subquery column",
        "",
        "MERGE INTO {t} t USING (SELECT id, {e} AS v FROM ice.ns.src) s ON t.id = s.id WHEN \
         MATCHED THEN UPDATE SET v = s.v",
    ),
    (
        "merge, not matched by source",
        "",
        "MERGE INTO {t} t USING (SELECT -1 AS id) s ON t.id = s.id WHEN NOT MATCHED BY SOURCE \
         THEN UPDATE SET v = {et}",
    ),
    (
        "update with a where",
        "",
        "UPDATE {t} SET v = {ec} WHERE id >= 0",
    ),
    ("update with no where", "", "UPDATE {t} SET v = {ec}"),
    (
        "merge, not matched insert, merge-on-read",
        "M",
        "MERGE INTO {t} t USING (SELECT id + 10 AS id, {source} FROM ice.ns.src) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT (id, v, k) VALUES (s.id, {es}, 0)",
    ),
    (
        "merge, matched update, merge-on-read",
        "M",
        "MERGE INTO {t} t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = {es}",
    ),
    (
        "merge, not matched by source, merge-on-read",
        "M",
        "MERGE INTO {t} t USING (SELECT -1 AS id) s ON t.id = s.id WHEN NOT MATCHED BY SOURCE \
         THEN UPDATE SET v = {et}",
    ),
    (
        "update with a where, merge-on-read",
        "M",
        "UPDATE {t} SET v = {ec} WHERE id >= 0",
    ),
    (
        "update with no where, merge-on-read",
        "M",
        "UPDATE {t} SET v = {ec}",
    ),
    (
        "insert into a branch",
        "B",
        "INSERT INTO {t}.branch_b1 (id, v, k) SELECT id, {e}, 0 FROM ice.ns.src",
    ),
    (
        "merge into a branch",
        "B",
        "MERGE INTO {t}.branch_b1 t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN UPDATE \
         SET v = {es}",
    ),
    (
        "update of a branch",
        "B",
        "UPDATE {t}.branch_b1 SET v = {ec} WHERE id >= 0",
    ),
    (
        "explain analyze insert",
        "",
        "EXPLAIN ANALYZE INSERT INTO {t} SELECT id, {e}, 0, {null} FROM ice.ns.src",
    ),
    (
        "prepare of an insert",
        "",
        "PREPARE narrowed AS INSERT INTO {t} SELECT id, {e}, 0, {null} FROM ice.ns.src",
    ),
    (
        "insert from a common table expression",
        "",
        "INSERT INTO {t} WITH q AS (SELECT id, {e} AS v FROM ice.ns.src) SELECT id, v, 0, {null} \
         FROM q",
    ),
    (
        "insert from a subquery",
        "",
        "INSERT INTO {t} (id, v, k) SELECT a.id, a.v, 0 FROM (SELECT b.id, b.v FROM (SELECT id, \
         {e} AS v FROM ice.ns.src) b) a",
    ),
    (
        "insert of a union",
        "",
        "INSERT INTO {t} (id, v, k) SELECT id, {source}, 0 FROM ice.ns.src WHERE id = 1 UNION ALL \
         SELECT id, {e}, 0 FROM ice.ns.src WHERE id > 1",
    ),
    (
        "insert of a join",
        "",
        "INSERT INTO {t} (id, v, k) SELECT l.id, r.v, 0 FROM ice.ns.src l JOIN (SELECT id, {e} AS \
         v FROM ice.ns.src) r ON l.id = r.id",
    ),
    (
        "insert of an aggregate",
        "",
        "INSERT INTO {t} (id, v, k) SELECT id, max({e}), 0 FROM ice.ns.src GROUP BY id",
    ),
    (
        "insert of a window",
        "",
        "INSERT INTO {t} (id, v, k) SELECT id, first_value({e}) OVER (PARTITION BY id ORDER BY \
         id), 0 FROM ice.ns.src",
    ),
    (
        "insert of a distinct",
        "",
        "INSERT INTO {t} (id, v, k) SELECT DISTINCT id, v, 0 FROM (SELECT id, {e} AS v FROM \
         ice.ns.src)",
    ),
    (
        "insert of a scalar subquery",
        "",
        "INSERT INTO {t} (id, v, k) SELECT o.id, (SELECT max({ei}) FROM ice.ns.src i WHERE i.id = \
         o.id), 0 FROM ice.ns.src o",
    ),
    (
        "insert from a temporary view",
        "V",
        "INSERT INTO {t} (id, v, k) SELECT id, v, 0 FROM narrowed_view",
    ),
];

fn session(zone: &str) -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect))
        .with_extension(Arc::new(SparkExtension))
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
        .build()
        .expect("session")
}

async fn attempt(session: &ReparkSession, sql: &str) -> Result<(), String> {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn run(session: &ReparkSession, sql: &str) {
    assert_eq!(attempt(session, sql).await, Ok(()), "{sql}");
}

async fn catalog(session: &ReparkSession) -> TempDir {
    let warehouse = TempDir::new().expect("warehouse");
    session
        .register_memory_catalog("ice", warehouse.path().to_str().expect("path"))
        .await
        .expect("catalog");
    run(session, "CREATE NAMESPACE ice.ns").await;
    run(
        session,
        "CREATE TABLE ice.ns.src (id INT, ns timestamp_ns, tzns timestamptz_ns) USING iceberg \
         TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let rows: Vec<String> = MOMENTS
        .iter()
        .enumerate()
        .map(|(index, (text, _))| {
            format!(
                "({}, CAST('{text}' AS timestamp_ns), CAST('{text}+00:00' AS timestamptz_ns))",
                index + 1
            )
        })
        .collect();
    run(
        session,
        &format!("INSERT INTO ice.ns.src VALUES {}", rows.join(", ")),
    )
    .await;
    warehouse
}

async fn ticks(session: &ReparkSession, sql: &str) -> Vec<Option<i64>> {
    let batches = session
        .sql(sql)
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    batches
        .iter()
        .flat_map(|batch| {
            let ints = cast(batch.column(0), &DataType::Int64).expect("ticks as int64");
            ints.as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<Option<i64>>>()
        })
        .collect()
}

fn data_files(root: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                data_files(&path)
            } else {
                usize::from(path.extension().is_some_and(|kind| kind == "parquet"))
            }
        })
        .sum()
}

struct Cell<'a> {
    session: &'a ReparkSession,
    warehouse: &'a Path,
    table: String,
    spelling: &'a str,
    source: (&'a str, &'a str),
    target: (&'a str, &'a str),
}

impl Cell<'_> {
    fn over(&self, column: &str, row: &str) -> String {
        self.spelling
            .replace("{c}", column)
            .replace("{i}", row)
            .replace("{n}", self.source.1)
    }

    fn statement(&self, template: &str) -> String {
        let (column, kind) = self.source;
        let moment = MOMENTS[0].0;
        let literal = if column == "ns" {
            format!("CAST('{moment}' AS timestamp_ns)")
        } else {
            format!("CAST('{moment}+00:00' AS timestamptz_ns)")
        };
        template
            .replace("{t}", &format!("ice.ns.{}", self.table))
            .replace("{literal}", &self.over(&literal, "9"))
            .replace("{es}", &self.over(&format!("s.{column}"), "s.id"))
            .replace("{et}", &self.over("t.c", "t.id"))
            .replace("{ec}", &self.over("c", "id"))
            .replace("{ei}", &self.over(&format!("i.{column}"), "i.id"))
            .replace("{e}", &self.over(column, "id"))
            .replace("{null}", &format!("CAST(NULL AS {kind})"))
            .replace("{source}", column)
    }

    async fn create(&self, flags: &str) {
        let layout = if flags.contains('P') {
            "PARTITIONED BY (k) "
        } else {
            ""
        };
        let mode = if flags.contains('M') { MOR } else { "" };
        let table = format!("ice.ns.{}", self.table);
        run(
            self.session,
            &format!(
                "CREATE TABLE {table} (id INT, v {}, k INT, c {}) USING iceberg \
                 {layout}TBLPROPERTIES ('format-version'='3'{mode})",
                self.target.0, self.source.1
            ),
        )
        .await;
        run(
            self.session,
            &format!(
                "INSERT INTO {table} (id, k, c) SELECT id, 0, {} FROM ice.ns.src",
                self.source.0
            ),
        )
        .await;
        if flags.contains('B') {
            run(
                self.session,
                &format!("ALTER TABLE {table} CREATE BRANCH b1"),
            )
            .await;
        }
        if flags.contains('V') {
            let view = self.statement(
                "CREATE OR REPLACE TEMPORARY VIEW narrowed_view AS SELECT id, {e} AS v FROM \
                 ice.ns.src",
            );
            run(self.session, &view).await;
        }
    }

    fn files(&self) -> usize {
        data_files(&self.warehouse.join("ns").join(&self.table))
    }

    async fn values(&self, reference: &str) -> Vec<Option<i64>> {
        let read = format!("SELECT v FROM ice.ns.{}{reference} ORDER BY id", self.table);
        ticks(self.session, &read).await
    }

    async fn refuses(&self, door: &str, flags: &str, template: &str) {
        let filtered = door.starts_with("update with a where");
        let zoned_subquery = door == "update from a scalar subquery"
            && self.spelling.starts_with("CAST(coalesce")
            && self.source.0 == "tzns"
            && self.target.0 == "timestamp_ns";
        let unsupported = if filtered && self.spelling.contains("AS {n})") {
            Some("Unsupported SQL type")
        } else if zoned_subquery {
            Some("Schema error: No field named")
        } else if filtered && self.spelling.contains("AS TIMESTAMP_NTZ)") {
            Some("[UNSUPPORTED_TIMESTAMP_NTZ]")
        } else {
            None
        };
        self.create(flags).await;
        let before = self.files();
        let write = self.statement(template);
        let refused = attempt(self.session, &write)
            .await
            .expect_err(&format!("{door}: {write}"));
        let cast = format!(
            "Cannot safely cast `v` \"TIMESTAMP\" to \"{}\"",
            self.target.1
        );
        let advice = if NARROWED_BESIDE_A_VALUE.contains(&self.spelling) {
            "was narrowed from nanoseconds to microseconds before the store, to match the \
             microsecond value beside it; write CAST("
                .to_string()
        } else {
            format!(
                "The value was narrowed from nanoseconds to microseconds before the store; give \
                 the NULL beside it the type {}",
                self.target.0
            )
        };
        let table = if door.starts_with("merge") {
            "the table ``:".to_string()
        } else if flags.contains('B') {
            "the table `datafusion.public.".to_string()
        } else if door.starts_with("insert overwrite") || door == "insert by name" {
            format!("the table `ns`.`{}`:", self.table)
        } else {
            format!("the table `ice.ns.{}`:", self.table)
        };
        let named = if let Some(unsupported) = unsupported {
            refused.contains(unsupported)
        } else if !refused.contains(&table) {
            false
        } else {
            refused.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]")
                && refused.contains(&cast)
                && refused.contains(&advice)
                && refused.contains("SQLSTATE: KD000")
        };
        assert!(named, "{door}: {write}: {refused}");
        assert_eq!(self.files(), before, "{door}: {write}");
        let empty = vec![None; MOMENTS.len()];
        assert_eq!(self.values("").await, empty, "{door}: {write}");
        if flags.contains('B') {
            assert_eq!(self.values(".branch_b1").await, empty, "{door}: {write}");
        }
    }
}

async fn every_door_refuses(zone: &str, spellings: &[&str]) {
    let session = session(zone);
    let warehouse = catalog(&session).await;
    let mut count = 0;
    for target in TARGETS {
        for source in SOURCES {
            for spelling in spellings {
                for (door, flags, template) in DOORS {
                    count += 1;
                    let cell = Cell {
                        session: &session,
                        warehouse: warehouse.path(),
                        table: format!("r{count}"),
                        spelling,
                        source,
                        target,
                    };
                    cell.refuses(door, flags, template).await;
                }
            }
        }
    }
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_utc() {
    every_door_refuses(ZONES[0], &NARROWED[..4]).await;
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_new_york() {
    every_door_refuses(ZONES[1], &NARROWED[..6]).await;
}

#[tokio::test]
async fn every_door_refuses_the_other_spellings_of_the_family() {
    every_door_refuses(ZONES[1], &NARROWED[6..]).await;
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_kolkata() {
    every_door_refuses(ZONES[2], &NARROWED[..4]).await;
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_kathmandu() {
    every_door_refuses(ZONES[3], &NARROWED[..4]).await;
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_an_untyped_null_in_lord_howe() {
    every_door_refuses(ZONES[4], &NARROWED[..4]).await;
}

#[tokio::test]
async fn a_call_that_widens_back_over_a_narrowed_value_does_not_make_it_store() {
    every_door_refuses(ZONES[1], &NARROWED_UNDER_A_WRITTEN_CALL).await;
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_a_typed_null_or_a_microsecond_value() {
    every_door_refuses(ZONES[1], &NARROWED_BESIDE_A_VALUE[..4]).await;
}

#[tokio::test]
async fn every_door_refuses_a_value_narrowed_beside_a_microsecond_literal() {
    every_door_refuses(ZONES[4], &NARROWED_BESIDE_A_VALUE[4..]).await;
}

async fn stored_by(
    session: &ReparkSession,
    table: &str,
    target: &str,
    write: &str,
) -> Vec<Option<i64>> {
    run(
        session,
        &format!(
            "CREATE TABLE ice.ns.{table} (id INT, v {target}) USING iceberg TBLPROPERTIES \
             ('format-version'='3')"
        ),
    )
    .await;
    run(session, &write.replace("{t}", &format!("ice.ns.{table}"))).await;
    ticks(
        session,
        &format!("SELECT v FROM ice.ns.{table} ORDER BY id"),
    )
    .await
}

async fn written_arms(zone: &str) {
    let session = session(zone);
    let warehouse = catalog(&session).await;
    let mut arms: Vec<(String, &str)> = UNITS
        .iter()
        .map(|unit| (format!("date_trunc('{unit}', {{x}})"), "zoned"))
        .collect();
    arms.push(("CAST({x} AS TIMESTAMP)".to_string(), "both"));
    arms.push(("CAST({x} AS DATE)".to_string(), "naive"));
    arms.push(("CAST({x} AS TIMESTAMP_NTZ)".to_string(), "none"));
    arms.push(("TRY_CAST({x} AS TIMESTAMP)".to_string(), "none"));
    arms.push(("CAST({x} AS {n})".to_string(), "none"));
    let narrowed = [
        "coalesce({c}, NULL)",
        "coalesce({c}, CAST(NULL AS TIMESTAMP))",
        "CASE WHEN id > 0 THEN {c} ELSE TIMESTAMP '2026-01-02 03:04:05' END",
    ];
    let mut count = 0;
    for (target, _) in TARGETS {
        for (source, kind) in SOURCES {
            for (arm, stores) in &arms {
                let over = |value: &str| arm.replace("{x}", value).replace("{n}", kind);
                let insert = |value: &str| {
                    format!(
                        "INSERT INTO {{t}} SELECT id, {} FROM ice.ns.src",
                        over(value)
                    )
                };
                count += 1;
                let reference =
                    stored_by(&session, &format!("a{count}"), target, &insert(source)).await;
                for inner in narrowed {
                    count += 1;
                    let table = format!("a{count}");
                    let write = insert(&inner.replace("{c}", source));
                    let equal = matches!(
                        (*stores, source),
                        ("both", _) | ("zoned", "tzns") | ("naive", "ns")
                    );
                    if equal {
                        let stored = stored_by(&session, &table, target, &write).await;
                        assert_eq!(stored, reference, "{zone}: {write}");
                        continue;
                    }
                    run(
                        &session,
                        &format!(
                            "CREATE TABLE ice.ns.{table} (id INT, v {target}) USING iceberg \
                             TBLPROPERTIES ('format-version'='3')"
                        ),
                    )
                    .await;
                    let refused =
                        attempt(&session, &write.replace("{t}", &format!("ice.ns.{table}")))
                            .await
                            .expect_err(&write);
                    assert!(
                        refused.contains("narrowed from nanoseconds to microseconds"),
                        "{zone}: {write}: {refused}"
                    );
                    assert_eq!(
                        data_files(&warehouse.path().join("ns").join(&table)),
                        0,
                        "{zone}: {write}"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn a_written_call_over_a_narrowed_value_stores_what_it_stores_over_the_nanosecond_value() {
    for zone in ZONES {
        written_arms(zone).await;
    }
}

#[test]
fn the_store_reads_the_mark_the_analyzer_places() {
    assert_eq!(
        repark_iceberg::write::narrowed_store::NARROWED_BESIDE_NULL_UDF_NAME,
        repark_functions::null_narrowing::NARROWED_BESIDE_NULL_NAME
    );
    assert_eq!(
        repark_iceberg::write::narrowed_store::NARROWED_BESIDE_VALUE_UDF_NAME,
        repark_functions::null_narrowing::NARROWED_BESIDE_VALUE_NAME
    );
    assert_eq!(
        repark_iceberg::write::narrowed_store::NARROW_TIMESTAMP_NS_UDF_NAME,
        repark_functions::timestamp_ns_cast::NARROW_TIMESTAMP_NS_NAME
    );
}

#[tokio::test]
async fn a_union_with_an_untyped_null_branch_refuses_and_a_typed_one_stores() {
    for zone in ZONES {
        let session = session(zone);
        let warehouse = catalog(&session).await;
        let mut count = 0;
        for target in TARGETS {
            for source in SOURCES {
                for (branch, refuses) in [
                    ("SELECT 9, NULL, 0", true),
                    ("SELECT 9 AS id, NULL AS v, 0 AS k", true),
                    ("SELECT 9, CAST(NULL AS TIMESTAMP), 0", true),
                    ("SELECT 9, TIMESTAMP '2026-01-02 03:04:05', 0", true),
                    ("SELECT 9, CAST(NULL AS {n}), 0", false),
                ] {
                    for order in [0, 1] {
                        count += 1;
                        let cell = Cell {
                            session: &session,
                            warehouse: warehouse.path(),
                            table: format!("u{count}"),
                            spelling: "{c}",
                            source,
                            target,
                        };
                        cell.create("").await;
                        let before = cell.files();
                        let rows = "SELECT id + 10, {e}, 0 FROM ice.ns.src";
                        let absent = branch.replace("{n}", source.1);
                        let query = if order == 0 {
                            format!("{rows} UNION ALL {absent}")
                        } else {
                            format!("{absent} UNION ALL {rows}")
                        };
                        let write =
                            cell.statement(&format!("INSERT INTO {{t}} (id, v, k) {query}"));
                        let outcome = attempt(&session, &write).await;
                        if refuses {
                            let refused = outcome.expect_err(&write);
                            assert!(
                                refused.contains("narrowed from nanoseconds to microseconds"),
                                "{write}: {refused}"
                            );
                            assert_eq!(cell.files(), before, "{write}");
                        } else {
                            assert_eq!(outcome, Ok(()), "{write}");
                            let stored: Vec<i64> =
                                cell.values("").await.into_iter().flatten().collect();
                            assert_eq!(stored.len(), MOMENTS.len(), "{write}");
                            assert!(
                                stored.iter().any(|tick| tick.rem_euclid(1_000) != 0),
                                "{write}: {stored:?}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn create_table_as_refuses_a_narrowed_value_it_would_type_as_nanoseconds() {
    let session = session(ZONES[1]);
    let warehouse = catalog(&session).await;
    for (index, head) in ["CREATE TABLE", "CREATE OR REPLACE TABLE"]
        .iter()
        .enumerate()
    {
        let table = format!("ice.ns.made{index}");
        let narrowed = format!(
            "{head} {table} USING iceberg TBLPROPERTIES ('format-version'='3') AS SELECT id, \
             array(ns, NULL)[0] AS v FROM ice.ns.src"
        );
        let refused = attempt(&session, &narrowed).await.expect_err(&narrowed);
        assert!(
            refused.contains("Cannot safely cast `v` \"TIMESTAMP\" to \"TIMESTAMP_NS\"")
                && refused.contains("narrowed from nanoseconds to microseconds"),
            "{narrowed}: {refused}"
        );
        assert_eq!(
            data_files(&warehouse.path().join("ns").join(format!("made{index}"))),
            0
        );
        assert!(
            attempt(&session, &format!("SELECT * FROM {table}"))
                .await
                .is_err()
        );
        let kept = format!(
            "{head} {table} USING iceberg TBLPROPERTIES ('format-version'='3') AS SELECT id, \
             array(ns, CAST(NULL AS timestamp_ns))[0] AS v FROM ice.ns.src"
        );
        run(&session, &kept).await;
        let read = format!("SELECT v FROM {table} ORDER BY id");
        assert_eq!(
            ticks(&session, &read).await,
            MOMENTS
                .iter()
                .map(|(_, tick)| Some(*tick))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn a_narrowing_the_statement_writes_and_a_kept_type_store_on_every_door() {
    let session = session(ZONES[1]);
    let warehouse = catalog(&session).await;
    let mut count = 0;
    for target in TARGETS {
        for source in SOURCES {
            for spelling in WRITTEN.iter().chain(&KEPT) {
                for (door, flags, template) in DOORS {
                    count += 1;
                    let cell = Cell {
                        session: &session,
                        warehouse: warehouse.path(),
                        table: format!("w{count}"),
                        spelling,
                        source,
                        target,
                    };
                    cell.create(flags).await;
                    let write = cell.statement(template);
                    let outcome = attempt(&session, &write).await;
                    let narrowed = outcome
                        .as_ref()
                        .err()
                        .is_some_and(|text| text.contains("narrowed from nanoseconds"));
                    let unfiltered =
                        door.starts_with("update with no where") || door == "update of a branch";
                    let beside_plain = door == "insert of a union" && WRITTEN.contains(spelling);
                    assert!(
                        !narrowed || unfiltered || beside_plain,
                        "{door}: {write}: {outcome:?}"
                    );
                    if outcome.is_err()
                        || door.starts_with("explain")
                        || door.starts_with("prepare")
                    {
                        continue;
                    }
                    let reference = if flags.contains('B') {
                        ".branch_b1"
                    } else {
                        ""
                    };
                    let stored: Vec<i64> =
                        cell.values(reference).await.into_iter().flatten().collect();
                    if KEPT.contains(spelling) {
                        assert!(
                            stored.iter().any(|tick| tick.rem_euclid(1_000) != 0),
                            "{door}: {write}: {stored:?}"
                        );
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn a_microsecond_target_takes_a_narrowed_value_as_before() {
    let session = session(ZONES[1]);
    let _warehouse = catalog(&session).await;
    for (index, kind) in ["TIMESTAMP", "TIMESTAMP_NTZ"].iter().enumerate() {
        let table = format!("ice.ns.micro{index}");
        run(
            &session,
            &format!(
                "CREATE TABLE {table} (id INT, v {kind}, w timestamp_ns) USING iceberg \
                 TBLPROPERTIES ('format-version'='3')"
            ),
        )
        .await;
        run(
            &session,
            &format!("INSERT INTO {table} SELECT id, coalesce(ns, NULL), ns FROM ice.ns.src"),
        )
        .await;
        run(
            &session,
            &format!(
                "MERGE INTO {table} t USING ice.ns.src s ON t.id = s.id WHEN MATCHED THEN UPDATE \
                 SET v = if(s.id > 0, s.ns, NULL)"
            ),
        )
        .await;
        run(
            &session,
            &format!("UPDATE {table} SET v = array(w, NULL)[0] WHERE id > 0"),
        )
        .await;
        let read = format!("SELECT w FROM {table} ORDER BY id");
        assert_eq!(
            ticks(&session, &read).await,
            MOMENTS
                .iter()
                .map(|(_, tick)| Some(*tick))
                .collect::<Vec<_>>()
        );
        let stored = ticks(&session, &format!("SELECT v FROM {table} ORDER BY id")).await;
        assert_eq!(stored.iter().flatten().count(), MOMENTS.len(), "{kind}");
    }
}

#[tokio::test]
async fn a_query_that_stores_nothing_answers_as_before() {
    let session = session(ZONES[1]);
    let _warehouse = catalog(&session).await;
    for spelling in NARROWED.iter().chain(&NARROWED_UNDER_A_WRITTEN_CALL) {
        let value = spelling
            .replace("{c}", "ns")
            .replace("{i}", "id")
            .replace("{n}", "timestamp_ns");
        let read = format!("SELECT {value} AS x FROM ice.ns.src ORDER BY id");
        let batches = session
            .sql(&read)
            .await
            .expect(&read)
            .collect()
            .await
            .expect(&read);
        let field = batches[0].schema().field(0).clone();
        assert_eq!(field.name(), "x", "{read}");
        assert!(field.metadata().is_empty(), "{read}: {field:?}");
        assert_eq!(
            batches
                .iter()
                .map(datafusion::arrow::array::RecordBatch::num_rows)
                .sum::<usize>(),
            MOMENTS.len()
        );
        run(
            &session,
            &format!("EXPLAIN INSERT INTO ice.ns.src SELECT id, {value}, tzns FROM ice.ns.src"),
        )
        .await;
    }
}
