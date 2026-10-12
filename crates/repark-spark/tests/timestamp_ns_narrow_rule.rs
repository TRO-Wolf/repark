use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_core::ReparkSession;
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

const ZONES: [&str; 3] = ["UTC", "America/New_York", "Australia/Lord_Howe"];
const MOMENTS: [(&str, i64); 5] = [
    ("2026-01-02 03:04:05.123456789", 1_767_323_045_123_456_789),
    ("2026-03-08 02:30:00.000000001", 1_772_937_000_000_000_001),
    ("1969-12-31 23:59:59.999999999", -1),
    ("1969-12-31 23:59:58.999999999", -1_000_000_001),
    ("1969-12-31 23:59:59.000000001", -999_999_999),
];
const SOURCES: [(&str, &str); 2] = [("ns", "timestamp_ns"), ("tzns", "timestamptz_ns")];
const TARGETS: [&str; 2] = ["timestamp_ns", "timestamptz_ns"];
const MOR: &str = ", 'write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', \
                   'write.merge.mode'='merge-on-read'";
const DOORS: [(&str, &str, &str); 7] = [
    ("insert", "", "INSERT INTO {t} (id, v) {q}"),
    ("insert overwrite", "S", "INSERT OVERWRITE {t} {q}"),
    (
        "merge insert",
        "",
        "MERGE INTO {t} t USING ({q}) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id, v) \
         VALUES (s.id, s.v)",
    ),
    (
        "merge update",
        "S",
        "MERGE INTO {t} t USING ({q}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v",
    ),
    (
        "merge update, merge-on-read",
        "SM",
        "MERGE INTO {t} t USING ({q}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v",
    ),
    (
        "insert from a temporary view",
        "V",
        "INSERT INTO {t} (id, v) SELECT id, v FROM fold_view",
    ),
    (
        "insert of a subquery",
        "",
        "INSERT INTO {t} (id, v) SELECT q.id, q.v FROM ({q}) q",
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
        "CREATE TABLE ice.ns.src (id INT, ns timestamp_ns, tzns timestamptz_ns, ts TIMESTAMP, \
         ntz TIMESTAMP_NTZ, d DATE) USING iceberg TBLPROPERTIES ('format-version'='3')",
    )
    .await;
    let rows: Vec<String> = MOMENTS
        .iter()
        .enumerate()
        .map(|(index, (text, _))| {
            format!(
                "({}, CAST('{text}' AS timestamp_ns), CAST('{text}+00:00' AS timestamptz_ns), \
                 TIMESTAMP '2001-02-03 04:05:06.654321', \
                 TIMESTAMP_NTZ '2001-02-03 04:05:06.654321', DATE '2001-02-03')",
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

fn files_ending(root: &Path, ending: &str) -> usize {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                files_ending(&path, ending)
            } else {
                usize::from(path.to_string_lossy().ends_with(ending))
            }
        })
        .sum()
}

struct Store<'a> {
    session: &'a ReparkSession,
    warehouse: &'a Path,
    table: String,
    target: &'a str,
}

impl Store<'_> {
    fn name(&self) -> String {
        format!("ice.ns.{}", self.table)
    }

    fn root(&self) -> std::path::PathBuf {
        self.warehouse.join("ns").join(&self.table)
    }

    async fn create(&self, flags: &str) {
        let mode = if flags.contains('M') { MOR } else { "" };
        run(
            self.session,
            &format!(
                "CREATE TABLE {} (id INT, v {}) USING iceberg TBLPROPERTIES \
                 ('format-version'='3'{mode})",
                self.name(),
                self.target
            ),
        )
        .await;
        if flags.contains('S') {
            run(
                self.session,
                &format!("INSERT INTO {} (id) SELECT id FROM ice.ns.src", self.name()),
            )
            .await;
        }
    }

    async fn write(&self, flags: &str, template: &str, query: &str) -> Result<(), String> {
        if flags.contains('V') {
            run(
                self.session,
                &format!("CREATE OR REPLACE TEMPORARY VIEW fold_view AS {query}"),
            )
            .await;
        }
        let statement = template.replace("{t}", &self.name()).replace("{q}", query);
        attempt(self.session, &statement).await
    }

    async fn stored(&self) -> Vec<Option<i64>> {
        ticks(
            self.session,
            &format!(
                "SELECT v FROM {} WHERE v IS NOT NULL ORDER BY id",
                self.name()
            ),
        )
        .await
    }

    async fn stores(&self, door: &str, flags: &str, template: &str, query: &str) -> Vec<i64> {
        self.create(flags).await;
        let outcome = self.write(flags, template, query).await;
        assert_eq!(outcome, Ok(()), "{door}: {query}");
        self.stored().await.into_iter().flatten().collect()
    }

    async fn refuses(&self, door: &str, flags: &str, template: &str, query: &str) -> String {
        self.create(flags).await;
        let data = files_ending(&self.root(), ".parquet");
        let commits = files_ending(&self.root(), ".metadata.json");
        let refused = self
            .write(flags, template, query)
            .await
            .expect_err(&format!("{door}: {query}"));
        assert!(
            refused.contains("[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST]")
                && refused.contains("narrowed from nanoseconds to microseconds")
                && refused.contains("SQLSTATE: KD000"),
            "{door}: {query}: {refused}"
        );
        let internal = ["__repark", "Timestamp(", "Interval", "the table ``"];
        assert!(
            !internal.iter().any(|text| refused.contains(text)),
            "{door}: {query}: {refused}"
        );
        assert_eq!(
            files_ending(&self.root(), ".parquet"),
            data,
            "{door}: {query}"
        );
        assert_eq!(
            files_ending(&self.root(), ".metadata.json"),
            commits,
            "{door}: {query}"
        );
        assert_eq!(self.stored().await, Vec::new(), "{door}: {query}");
        run(
            self.session,
            &format!(
                "INSERT INTO {} (id, v) SELECT 100, {} FROM ice.ns.src WHERE id = 1",
                self.name(),
                if self.target == "timestamp_ns" {
                    "ns"
                } else {
                    "tzns"
                }
            ),
        )
        .await;
        assert_eq!(self.stored().await.len(), 1, "{door}: {query}");
        refused
    }
}

struct Matrix {
    session: ReparkSession,
    warehouse: TempDir,
    count: usize,
}

impl Matrix {
    async fn open(zone: &str) -> Self {
        let session = session(zone);
        let warehouse = catalog(&session).await;
        Self {
            session,
            warehouse,
            count: 0,
        }
    }

    fn store<'a>(&'a mut self, target: &'a str) -> Store<'a> {
        self.count += 1;
        Store {
            session: &self.session,
            warehouse: self.warehouse.path(),
            table: format!("f{}", self.count),
            target,
        }
    }
}

fn select(value: &str) -> String {
    format!("SELECT id, {value} AS v FROM ice.ns.src")
}

#[tokio::test]
async fn a_written_cast_beside_a_microsecond_value_stores_on_every_door() {
    let spellings = [
        "coalesce(CAST({c} AS TIMESTAMP), ts)",
        "array(CAST({c} AS TIMESTAMP), ts)[0]",
        "CASE WHEN id > 0 THEN CAST({c} AS TIMESTAMP) ELSE ts END",
        "coalesce(CAST({c} AS TIMESTAMP), DATE '2001-02-03')",
        "coalesce(CAST({c} AS TIMESTAMP), TIMESTAMP '2001-02-03 04:05:06.654321')",
        "greatest(CAST({c} AS TIMESTAMP), CAST('1500-02-03 04:05:06' AS TIMESTAMP))",
    ];
    for zone in ZONES {
        let mut matrix = Matrix::open(zone).await;
        for target in TARGETS {
            for (source, _) in SOURCES {
                for (door, flags, template) in DOORS {
                    let twin = select(&format!("CAST({source} AS TIMESTAMP)"));
                    let reference = matrix
                        .store(target)
                        .stores(door, flags, template, &twin)
                        .await;
                    assert_eq!(reference.len(), MOMENTS.len(), "{zone}: {door}: {twin}");
                    for spelling in spellings {
                        let query = select(&spelling.replace("{c}", source));
                        let stored = matrix
                            .store(target)
                            .stores(door, flags, template, &query)
                            .await;
                        assert_eq!(stored, reference, "{zone}: {door}: {query}");
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn a_union_of_written_casts_and_a_values_row_with_a_written_cast_store() {
    for zone in [ZONES[0], ZONES[1]] {
        let mut matrix = Matrix::open(zone).await;
        for target in TARGETS {
            for (source, kind) in SOURCES {
                let part = format!("SELECT id, CAST({source} AS TIMESTAMP) AS v FROM ice.ns.src");
                let three = format!(
                    "{part} WHERE id % 3 = 0 UNION ALL {part} WHERE id % 3 = 1 UNION ALL {part} \
                     WHERE id % 3 = 2"
                );
                let above = format!(
                    "SELECT id, coalesce(v, ts) AS v FROM ({three}) u JOIN (SELECT \
                     id AS i, ts FROM ice.ns.src) s ON u.id = s.i"
                );
                for (door, flags, template) in DOORS {
                    let reference = matrix
                        .store(target)
                        .stores(door, flags, template, &part)
                        .await;
                    for query in [&three, &above] {
                        let stored = matrix
                            .store(target)
                            .stores(door, flags, template, query)
                            .await;
                        assert_eq!(stored, reference, "{zone}: {door}: {query}");
                    }
                }
                let zone_suffix = if kind == "timestamptz_ns" {
                    "+00:00"
                } else {
                    ""
                };
                let literal = format!("CAST('{}{zone_suffix}' AS {kind})", MOMENTS[0].0);
                let alone =
                    format!("INSERT INTO {{t}} (id, v) VALUES (1, CAST({literal} AS TIMESTAMP))");
                let reference = matrix.store(target).stores("values", "", &alone, "").await;
                assert_eq!(reference.len(), 1, "{zone}: {alone}");
                for sibling in [
                    "TIMESTAMP '2001-02-03 04:05:06.654321'",
                    "CAST('2001-02-03 04:05:06' AS TIMESTAMP)",
                    "CAST(NULL AS TIMESTAMP)",
                ] {
                    let beside = format!(
                        "INSERT INTO {{t}} (id, v) VALUES (1, coalesce(CAST({literal} AS \
                         TIMESTAMP), {sibling}))"
                    );
                    let stored = matrix.store(target).stores("values", "", &beside, "").await;
                    assert_eq!(stored, reference, "{zone}: {beside}");
                }
            }
        }
    }
}

#[tokio::test]
async fn a_plain_column_stores_beside_a_narrowing_under_rollup_cube_and_lateral() {
    let queries = [
        "SELECT id, max({c}) AS v FROM ice.ns.src WHERE coalesce({c}, ts) IS NOT NULL GROUP BY \
         ROLLUP (id) HAVING id IS NOT NULL",
        "SELECT id, v FROM (SELECT id, max({c}) AS v, count(coalesce({c}, ts)) AS n FROM \
         ice.ns.src GROUP BY CUBE (id) HAVING id IS NOT NULL) g",
        "SELECT id, v FROM (SELECT id, max({c}) AS v, max(coalesce({c}, ts)) AS w FROM \
         ice.ns.src GROUP BY ROLLUP (id) HAVING id IS NOT NULL) g",
        "SELECT o.id, l.v FROM ice.ns.src o, LATERAL (SELECT o.{c} AS v, coalesce(o.{c}, o.ts) \
         AS w) l",
        "SELECT o.id, l.v FROM ice.ns.src o, LATERAL (SELECT o.{c} AS v) l WHERE \
         coalesce(o.{c}, NULL) IS NOT NULL",
    ];
    let whole: Vec<i64> = MOMENTS.iter().map(|(_, nanos)| *nanos).collect();
    for zone in [ZONES[0], ZONES[1]] {
        let mut matrix = Matrix::open(zone).await;
        for (source, kind) in SOURCES {
            for (door, flags, template) in DOORS {
                for query in queries {
                    let query = query.replace("{c}", source);
                    let stored = matrix
                        .store(kind)
                        .stores(door, flags, template, &query)
                        .await;
                    assert_eq!(stored, whole, "{zone}: {door}: {query}");
                }
            }
        }
    }
}

async fn every_door_refuses(zone: &str, queries: &[String]) {
    let mut matrix = Matrix::open(zone).await;
    for target in TARGETS {
        for (door, flags, template) in DOORS {
            for query in queries {
                matrix
                    .store(target)
                    .refuses(door, flags, template, query)
                    .await;
            }
        }
    }
}

fn over_both_sources(spellings: &[&str]) -> Vec<String> {
    SOURCES
        .iter()
        .flat_map(|(source, kind)| {
            spellings
                .iter()
                .map(move |spelling| select(&spelling.replace("{c}", source).replace("{n}", kind)))
        })
        .collect()
}

#[tokio::test]
async fn a_value_narrowed_inside_a_container_refuses_on_every_door() {
    let queries = over_both_sources(&[
        "coalesce(array({c}), array(ts))[0]",
        "array_append(array({c}), ts)[0]",
        "array_prepend(array({c}), ts)[1]",
        "array_union(array({c}), array(ts))[0]",
        "flatten(array(array({c}), array(ts)))[0]",
        "(CASE WHEN id > 0 THEN array({c}) ELSE array(ts) END)[0]",
    ]);
    every_door_refuses(ZONES[1], &queries).await;
}

#[tokio::test]
async fn a_value_narrowed_beside_a_date_refuses_on_every_door() {
    let mut queries = over_both_sources(&[
        "coalesce({c}, DATE '2001-02-03')",
        "coalesce({c}, CAST(NULL AS DATE))",
        "coalesce({c}, d)",
        "CASE WHEN id > 0 THEN {c} ELSE d END",
    ]);
    for (source, _) in SOURCES {
        queries.push(format!(
            "SELECT id, {source} AS v FROM ice.ns.src UNION ALL SELECT 100, DATE '2001-02-03'"
        ));
    }
    every_door_refuses(ZONES[2], &queries).await;
}

#[tokio::test]
async fn a_value_narrowed_over_an_outer_reference_or_a_lambda_variable_refuses() {
    let mut queries = Vec::new();
    for (source, _) in SOURCES {
        for inner in [
            "coalesce(o.{c}, CAST(NULL AS TIMESTAMP))",
            "CASE WHEN o.id > 0 THEN o.{c} ELSE TIMESTAMP '2001-02-03 04:05:06.654321' END",
            "coalesce(o.{c}, NULL)",
            "coalesce(o.{c}, o.ts)",
        ] {
            queries.push(format!(
                "SELECT o.id, l.v FROM ice.ns.src o, LATERAL (SELECT {} AS v) l",
                inner.replace("{c}", source)
            ));
        }
        queries.push(format!(
            "SELECT o.id, l.v FROM (SELECT id, coalesce({source}, ts) AS w FROM ice.ns.src) o, \
             LATERAL (SELECT o.w AS v) l"
        ));
    }
    every_door_refuses(ZONES[1], &queries).await;
    let mut matrix = Matrix::open(ZONES[1]).await;
    for target in TARGETS {
        for (literal, sibling) in [
            (
                "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns)",
                "TIMESTAMP '2001-02-03 04:05:06.654321'",
            ),
            (
                "CAST('2026-01-02 03:04:05.123456789+00:00' AS timestamptz_ns)",
                "TIMESTAMP '2001-02-03 04:05:06.654321'",
            ),
            (
                "CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns)",
                "CAST(NULL AS TIMESTAMP)",
            ),
        ] {
            let template = format!(
                "INSERT INTO {{t}} (id, v) VALUES (1, transform(array({literal}), x -> \
                 coalesce(x, {sibling}))[0])"
            );
            matrix
                .store(target)
                .refuses("insert values", "", &template, "")
                .await;
        }
    }
}

#[tokio::test]
async fn a_narrowed_value_that_leaves_the_timestamp_type_and_returns_refuses() {
    let queries = over_both_sources(&[
        "CAST(CAST(coalesce({c}, NULL) AS STRING) AS {n})",
        "CAST(CAST(coalesce({c}, ts) AS STRING) AS {n})",
        "to_timestamp(CAST(coalesce({c}, ts) AS STRING))",
        "timestamp_micros(unix_micros(coalesce({c}, ts)))",
        "timestamp_micros(CAST(unix_micros(coalesce({c}, NULL)) AS BIGINT))",
        "CAST(concat_ws('', CAST(coalesce({c}, ts) AS STRING)) AS {n})",
        "CAST(get_json_object(to_json(named_struct('f', coalesce({c}, ts))), '$.f') AS {n})",
    ]);
    every_door_refuses(ZONES[1], &queries).await;
}

#[tokio::test]
async fn a_written_call_does_not_forgive_a_cut_it_does_not_equal() {
    let mut queries: Vec<String> = [
        "CAST(coalesce(ns, ntz) AS TIMESTAMP)",
        "CAST(coalesce(ns, CAST(NULL AS TIMESTAMP_NTZ)) AS TIMESTAMP)",
        "CAST(coalesce(ns, ntz) AS DATE)",
        "CAST(coalesce(ns, CAST(NULL AS TIMESTAMP_NTZ)) AS DATE)",
        "CAST(CAST(coalesce(ns, ntz) AS TIMESTAMP) AS TIMESTAMP)",
        "date_trunc('second', CAST(coalesce(ns, ntz) AS TIMESTAMP))",
        "CAST(date_trunc('second', coalesce(ns, NULL)) AS TIMESTAMP)",
        "CAST(date_trunc('second', coalesce(ns, ts)) AS DATE)",
        "CAST(date_trunc('second', coalesce(ns, CAST(NULL AS TIMESTAMP))) AS TIMESTAMP)",
        "date_trunc('month', CAST(coalesce(tzns, NULL) AS DATE))",
        "CAST(CAST(coalesce(tzns, ts) AS DATE) AS TIMESTAMP)",
        "CAST(coalesce(ns, ts) + INTERVAL '0' SECOND AS TIMESTAMP)",
    ]
    .iter()
    .map(|value| select(value))
    .collect();
    queries.push(
        "SELECT id, CAST(v AS DATE) AS v FROM (SELECT id, coalesce(ns, ntz) AS v FROM ice.ns.src) q"
            .to_string(),
    );
    for zone in [ZONES[0], ZONES[1]] {
        every_door_refuses(zone, &queries).await;
    }
}

#[tokio::test]
async fn the_innermost_written_call_over_a_narrowed_value_decides() {
    let arms = [
        ("CAST(date_trunc('second', {x}) AS TIMESTAMP)", "tzns"),
        ("CAST(date_trunc('second', {x}) AS DATE)", "tzns"),
        ("date_trunc('day', date_trunc('second', {x}))", "tzns"),
        ("date_trunc('month', CAST({x} AS DATE))", "ns"),
        ("CAST(CAST({x} AS DATE) AS TIMESTAMP)", "ns"),
        ("date_trunc('second', CAST({x} AS TIMESTAMP))", "ns"),
        ("date_trunc('second', CAST({x} AS TIMESTAMP))", "tzns"),
        ("CAST(CAST({x} AS TIMESTAMP) AS TIMESTAMP)", "ns"),
        ("coalesce(CAST({x} AS TIMESTAMP), NULL)", "tzns"),
        ("CAST({x} AS TIMESTAMP)", "ns"),
        ("CAST({x} AS TIMESTAMP)", "tzns"),
    ];
    let narrowed = [
        "coalesce({c}, NULL)",
        "coalesce({c}, ts)",
        "CASE WHEN id > 0 THEN {c} ELSE TIMESTAMP '2001-02-03 04:05:06.654321' END",
        "coalesce({c}, CAST(NULL AS TIMESTAMP))",
    ];
    let (door, flags, template) = DOORS[0];
    for zone in ZONES {
        let mut matrix = Matrix::open(zone).await;
        for target in TARGETS {
            for (arm, source) in arms {
                let twin = select(&arm.replace("{x}", source));
                let reference = matrix
                    .store(target)
                    .stores(door, flags, template, &twin)
                    .await;
                for inner in narrowed {
                    let query = select(&arm.replace("{x}", &inner.replace("{c}", source)));
                    let stored = matrix
                        .store(target)
                        .stores(door, flags, template, &query)
                        .await;
                    assert_eq!(stored, reference, "{zone}: {query}");
                }
            }
        }
    }
}

#[tokio::test]
async fn an_update_through_a_qualified_set_column_refuses_in_both_modes() {
    let session = session(ZONES[1]);
    let warehouse = catalog(&session).await;
    let mut count = 0;
    for target in TARGETS {
        for (source, kind) in SOURCES {
            for mode in ["", MOR] {
                for statement in [
                    "UPDATE {t} AS u SET u.v = coalesce(u.c, NULL) WHERE u.id >= 0",
                    "UPDATE {t} SET u.v = coalesce(c, ts) WHERE id >= 0",
                    "UPDATE {t} AS u SET u.v = coalesce(u.c, CAST(NULL AS TIMESTAMP))",
                    "UPDATE {t} u SET u.v = greatest(u.c, u.ts) WHERE u.id >= 0",
                    "UPDATE {t} SET {t}.v = coalesce(c, ts) WHERE id >= 0",
                ] {
                    count += 1;
                    let table = format!("u{count}");
                    let name = format!("ice.ns.{table}");
                    run(
                        &session,
                        &format!(
                            "CREATE TABLE {name} (id INT, v {target}, c {kind}, ts TIMESTAMP) \
                             USING iceberg TBLPROPERTIES ('format-version'='3'{mode})"
                        ),
                    )
                    .await;
                    run(
                        &session,
                        &format!(
                            "INSERT INTO {name} (id, c, ts) SELECT id, {source}, ts FROM \
                             ice.ns.src"
                        ),
                    )
                    .await;
                    let root = warehouse.path().join("ns").join(&table);
                    let data = files_ending(&root, ".parquet");
                    let commits = files_ending(&root, ".metadata.json");
                    let write = statement
                        .replace("{t}.v", &format!("{table}.v"))
                        .replace("{t} SET u.v", &format!("{name} AS u SET u.v"))
                        .replace("{t}", &name);
                    let refused = attempt(&session, &write).await.expect_err(&write);
                    assert!(
                        refused.contains("narrowed from nanoseconds to microseconds")
                            && refused.contains(&format!("`{name}`"))
                            && !refused.contains("__repark"),
                        "{write}: {refused}"
                    );
                    assert_eq!(files_ending(&root, ".parquet"), data, "{write}");
                    assert_eq!(files_ending(&root, ".metadata.json"), commits, "{write}");
                    let read = format!("SELECT v FROM {name} WHERE v IS NOT NULL ORDER BY id");
                    assert_eq!(ticks(&session, &read).await, Vec::new(), "{write}");
                    run(
                        &session,
                        &format!("UPDATE {name} AS u SET u.v = u.c WHERE u.id >= 0"),
                    )
                    .await;
                    let stored: Vec<i64> =
                        ticks(&session, &read).await.into_iter().flatten().collect();
                    if (kind == "timestamp_ns") == (target == "timestamp_ns") {
                        let whole: Vec<i64> = MOMENTS.iter().map(|(_, nanos)| *nanos).collect();
                        assert_eq!(stored, whole, "{write}");
                    } else {
                        assert_eq!(stored.len(), MOMENTS.len(), "{write}");
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn a_written_cast_over_a_column_that_holds_the_narrowing_stores_as_the_inline_cast_does() {
    let narrowed = ["coalesce({c}, ts)", "coalesce({c}, NULL)"];
    for zone in [ZONES[1], ZONES[2]] {
        let mut matrix = Matrix::open(zone).await;
        for target in TARGETS {
            for (source, _) in SOURCES {
                for (door, flags, template) in DOORS {
                    let twin = select(&format!("CAST({source} AS TIMESTAMP)"));
                    let reference = matrix
                        .store(target)
                        .stores(door, flags, template, &twin)
                        .await;
                    for inner in narrowed {
                        let inner = inner.replace("{c}", source);
                        run(
                            &matrix.session,
                            &format!(
                                "CREATE OR REPLACE TEMPORARY VIEW narrowed_view AS SELECT id, \
                                 {inner} AS v FROM ice.ns.src"
                            ),
                        )
                        .await;
                        for query in [
                            format!("SELECT id, CAST({inner} AS TIMESTAMP) AS v FROM ice.ns.src"),
                            format!(
                                "SELECT id, CAST(v AS TIMESTAMP) AS v FROM (SELECT id, {inner} \
                                 AS v FROM ice.ns.src) q"
                            ),
                            "SELECT id, CAST(v AS TIMESTAMP) AS v FROM narrowed_view".to_string(),
                            format!(
                                "WITH q AS (SELECT id, {inner} AS v FROM ice.ns.src) SELECT \
                                 id, CAST(q.v AS TIMESTAMP) AS v FROM q"
                            ),
                        ] {
                            let stored = matrix
                                .store(target)
                                .stores(door, flags, template, &query)
                                .await;
                            assert_eq!(stored, reference, "{zone}: {door}: {query}");
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn to_date_and_date_answer_as_a_date_cast_does() {
    let (door, flags, template) = DOORS[0];
    for zone in ZONES {
        let mut matrix = Matrix::open(zone).await;
        for target in TARGETS {
            let twin = select("CAST(ns AS DATE)");
            let reference = matrix
                .store(target)
                .stores(door, flags, template, &twin)
                .await;
            for call in ["to_date({x})", "date({x})", "CAST({x} AS DATE)"] {
                for inner in [
                    "coalesce(ns, NULL)",
                    "coalesce(ns, ts)",
                    "coalesce(ns, CAST(NULL AS TIMESTAMP))",
                ] {
                    let query = select(&call.replace("{x}", inner));
                    let stored = matrix
                        .store(target)
                        .stores(door, flags, template, &query)
                        .await;
                    assert_eq!(stored, reference, "{zone}: {query}");
                }
                for inner in [
                    "coalesce(tzns, NULL)",
                    "coalesce(tzns, ts)",
                    "coalesce(ns, ntz)",
                ] {
                    let query = select(&call.replace("{x}", inner));
                    matrix
                        .store(target)
                        .refuses(door, flags, template, &query)
                        .await;
                }
            }
        }
    }
}

#[tokio::test]
async fn a_narrowed_aggregate_under_grouping_sets_refuses_and_its_plain_sibling_stores() {
    let whole: Vec<i64> = MOMENTS.iter().map(|(_, nanos)| *nanos).collect();
    let mut matrix = Matrix::open(ZONES[1]).await;
    for (source, kind) in SOURCES {
        for sets in ["ROLLUP (id)", "CUBE (id)", "GROUPING SETS ((id), ())"] {
            let inner = format!(
                "SELECT id, max({source}) AS p, max(coalesce({source}, ts)) AS c FROM \
                 ice.ns.src GROUP BY {sets} HAVING id IS NOT NULL"
            );
            for (door, flags, template) in DOORS {
                let plain = format!("SELECT id, p AS v FROM ({inner}) g");
                let stored = matrix
                    .store(kind)
                    .stores(door, flags, template, &plain)
                    .await;
                assert_eq!(stored, whole, "{door}: {plain}");
                let narrowed = format!("SELECT id, c AS v FROM ({inner}) g");
                matrix
                    .store(kind)
                    .refuses(door, flags, template, &narrowed)
                    .await;
            }
        }
    }
}

#[tokio::test]
async fn the_refusal_names_what_the_statement_wrote() {
    let mut matrix = Matrix::open(ZONES[1]).await;
    let (door, flags, template) = DOORS[0];
    for (value, named, remedy) in [
        (
            "coalesce(ns, ts)",
            "The value ice.ns.src.ns was narrowed",
            "write CAST(ice.ns.src.ns AS TIMESTAMP) if",
        ),
        (
            "coalesce(ns + INTERVAL '1' DAY, ts)",
            "The value was narrowed",
            "write a CAST to TIMESTAMP over the nanosecond value if",
        ),
        (
            "greatest(CAST(coalesce(ns, ntz) AS timestamp_ns), ts)",
            "The value was narrowed",
            "write a CAST to TIMESTAMP over the nanosecond value if",
        ),
        (
            "transform(array(ns), x -> coalesce(x, ts))[0]",
            "The value was narrowed",
            "write a CAST to TIMESTAMP over the nanosecond value if",
        ),
        (
            "coalesce(array(ns), array(ts))[0]",
            "The value was narrowed",
            "write a CAST to TIMESTAMP over the nanosecond value if",
        ),
        (
            "coalesce(CAST(tzns AS timestamp_ns), TIMESTAMP '2001-02-03 04:05:06')",
            "The value was narrowed",
            "write a CAST to TIMESTAMP over the nanosecond value if",
        ),
    ] {
        let refused = matrix
            .store("timestamp_ns")
            .refuses(door, flags, template, &select(value))
            .await;
        assert!(
            refused.contains(named) && refused.contains(remedy),
            "{value}: {refused}"
        );
    }
    for (door, flags, template) in DOORS {
        let store = matrix.store("timestamptz_ns");
        let table = store.name();
        let refused = store
            .refuses(door, flags, template, &select("coalesce(tzns, ts)"))
            .await;
        let bare = table.rsplit('.').next().unwrap_or_default();
        assert!(
            refused.contains(&format!("`{table}`")) || refused.contains(&format!("`{bare}`")),
            "{door}: {refused}"
        );
    }
}

#[tokio::test]
async fn a_read_shows_no_mark_no_tag_and_keeps_its_field_metadata() {
    let session = session(ZONES[1]);
    let _warehouse = catalog(&session).await;
    let metadata = |sql: String| {
        let session = &session;
        async move {
            let frame = session.sql(&sql).await.expect("read");
            frame.schema().field(1).metadata().clone()
        }
    };
    let plain = metadata("SELECT id, ns AS v FROM ice.ns.src".to_string()).await;
    assert!(!plain.is_empty(), "{plain:?}");
    for branch in [
        "SELECT 0, NULL",
        "SELECT 0, TIMESTAMP '2001-02-03 04:05:06'",
        "SELECT 0, CAST(NULL AS TIMESTAMP)",
        "SELECT 0, DATE '2001-02-03'",
    ] {
        let sql = format!("SELECT id, ns AS v FROM ice.ns.src UNION ALL {branch}");
        assert_eq!(metadata(sql.clone()).await, plain, "{sql}");
    }
    for read in [
        "SELECT id, coalesce(ns, ts) AS v FROM ice.ns.src",
        "SELECT id, coalesce(tzns, NULL) FROM ice.ns.src",
        "SELECT id, ns AS v FROM ice.ns.src UNION ALL SELECT 0, NULL",
        "SELECT id, CAST(v AS TIMESTAMP) FROM (SELECT id, coalesce(ns, ts) AS v FROM ice.ns.src) q",
        "SELECT id, array_append(array(ns), ts)[0] FROM ice.ns.src",
        "INSERT INTO ice.ns.src (id, ns) SELECT id, coalesce(ns, ts) FROM ice.ns.src",
    ] {
        for explain in [
            "EXPLAIN",
            "EXPLAIN VERBOSE",
            "EXPLAIN EXTENDED",
            "EXPLAIN FORMATTED",
        ] {
            let sql = format!("{explain} {read}");
            let Ok(frame) = session.sql(&sql).await else {
                continue;
            };
            let batches = frame.collect().await.expect(&sql);
            let shown = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
                .expect("text")
                .to_string();
            assert!(
                !shown.contains("__repark_narrowed") && !shown.contains("repark.untyped_null"),
                "{sql}: {shown}"
            );
        }
    }
}
