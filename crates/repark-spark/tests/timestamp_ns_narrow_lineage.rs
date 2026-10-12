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
async fn an_explain_frame_is_a_statement_and_prints_no_hidden_function() {
    let session = session(ZONES[1]);
    let _warehouse = catalog(&session).await;
    run(
        &session,
        "CREATE OR REPLACE TEMPORARY VIEW cut_view AS SELECT id, ts, coalesce(ns, ts) AS v FROM \
         ice.ns.src",
    )
    .await;
    let frame = session.sql("EXPLAIN SELECT 1 AS id").await.expect("frame");
    assert!(
        matches!(
            frame.logical_plan(),
            datafusion::logical_expr::LogicalPlan::Explain(_)
        ),
        "{}",
        frame.logical_plan().display_indent()
    );
    let hidden = [
        "__repark_narrowed_beside",
        "__repark_written_timestamp_cast__",
        "repark.untyped_null",
        "repark.written",
    ];
    for read in [
        "SELECT id, coalesce(ns, ts) AS v FROM ice.ns.src",
        "SELECT id, array_append(array(ns), ts)[0], coalesce(tzns, DATE '2001-02-03') FROM \
         ice.ns.src",
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
                !hidden.iter().any(|name| shown.contains(name)),
                "{sql}: {shown}"
            );
        }
    }
}
