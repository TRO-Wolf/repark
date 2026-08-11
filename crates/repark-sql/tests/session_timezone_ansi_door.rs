//! **The ANSI-door cell** of the session-timezone matrix (H-1a split B).
//!
//! The campaign narrows `docs/testing.md`'s matrix row 3 into four cells — native `DataFrame`,
//! ANSI door, Spark door, facade. Three live elsewhere
//! (`crates/repark-spark/tests/session_timezone.rs` and the facade corpus); this file is the
//! ANSI one, and it lives HERE for a policy reason worth stating: `scripts/check_crate_dag.py`
//! allows `repark-sql -> repark-spark` as a **dev** edge (that is what the cross-door protocol
//! needs) and allows no edge the other way at all, so the ANSI door and the Spark extension can
//! only meet inside this crate's test binary.
//!
//! # What is being claimed, precisely
//!
//! Extensions are **session-scoped, not dialect-scoped** (`tests/cross_door.rs` is emphatic about
//! this, and it is the whole reason the cross-door protocol runs two sessions). The session
//! timezone rides on the session's `ConfigOptions`, so the claim this file pins is:
//!
//! > on ONE Spark-extended session at a non-UTC zone, the ANSI door (`sql_with(AnsiDialect)`)
//! > resolves timestamp fields in that zone exactly as the Spark door does.
//!
//! That is a **single-session** row on purpose, and it is the legal kind: the two doors are being
//! compared on a surface the analyzer/UDF layer owns, and what is measured is that the door
//! choice does NOT change the answer. Running it as a two-session row would compare a
//! Spark-extended session against a native one — which have different function registries — and
//! would be measuring the extension, not the door.
//!
//! The native (extension-free) profile is pinned too, as the honest negative: stock DataFusion
//! has no session-timezone notion here, so a bare session keeps reading the stored zone. That is
//! not a repark divergence from Spark — a bare session is not a Spark session — and saying so
//! with a test keeps the boundary from being read as a gap.
//!
//! AWS-free by construction.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, TimeUnit};
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use repark_sql::AnsiDialect;

const NEW_YORK: &str = "America/New_York";
const TOKYO: &str = "Asia/Tokyo";

/// `2024-06-15T12:00:00Z` — the census's four-hour offset instant — and `2024-01-01T04:30:00Z`,
/// which crosses a calendar YEAR in New York.
const INSTANTS: [&str; 2] = ["2024-06-15T12:00:00Z", "2024-01-01T04:30:00Z"];

/// The instants under test, as RFC-3339 strings so an expectation is checkable by eye.
///
/// They are converted with arrow's own string→timestamp cast rather than a date library: the
/// fixture then cannot disagree with the engine about what `2024-06-15T12:00:00Z` means, and this
/// test binary needs no clock dependency of its own.
fn utc_instants(rfc3339: &[&str]) -> ArrayRef {
    let text = StringArray::from(rfc3339.to_vec());
    cast(
        &text,
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
    )
    .expect("well-formed RFC-3339 instant literals")
}

/// A Spark-extended session at `zone`, with the tz-aware instants registered as table `t`.
fn spark_extended_session_at(zone: &str) -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .config(repark_core::SESSION_TIME_ZONE_KEY, zone)
        .build()
        .expect("a Spark-extended session at a real zone");
    register_instants(&session);
    session
}

fn register_instants(session: &ReparkSession) {
    let column = utc_instants(&INSTANTS);
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )]));
    session
        .context()
        .register_batch(
            "t",
            RecordBatch::try_new(schema, vec![column]).expect("a one-column batch"),
        )
        .expect("register the instant table");
}

/// Run `sql` through `dialect` on `session`, returning `(Arrow types, i32 columns)`.
async fn int_columns_through(
    session: &ReparkSession,
    dialect: &Arc<dyn SqlDialect>,
    sql: &str,
) -> (Vec<DataType>, Vec<Vec<i32>>) {
    let batches = session
        .sql_with(dialect, sql)
        .await
        .unwrap_or_else(|error| panic!("plan/execute `{sql}`: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect `{sql}`: {error}"));
    let batch = &batches[0];
    let types = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.data_type().clone())
        .collect();
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let values = column.as_primitive::<Int32Type>();
            (0..values.len()).map(|row| values.value(row)).collect()
        })
        .collect();
    (types, columns)
}

const EXTRACT_SQL: &str = "SELECT year(ts) AS y, hour(ts) AS h FROM t ORDER BY ts";

/// The ANSI-door cell: the same session, the same data, both doors — and the same answer, value
/// AND Arrow type. If the zone ever reached only the Spark door's own routing (rather than the
/// session's function layer), the two halves would disagree here.
#[tokio::test]
async fn ansi_door_and_spark_door_agree_under_a_non_utc_session() {
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let spark: Arc<dyn SqlDialect> = Arc::new(SparkDialect);

    let new_york = spark_extended_session_at(NEW_YORK);
    let (ansi_types, ansi_columns) = int_columns_through(&new_york, &ansi, EXTRACT_SQL).await;
    let (spark_types, spark_columns) = int_columns_through(&new_york, &spark, EXTRACT_SQL).await;
    assert_eq!(ansi_types, vec![DataType::Int32; 2]);
    assert_eq!(
        ansi_columns,
        vec![vec![2023, 2024], vec![23, 8]],
        "2024-01-01T04:30Z is 2023-12-31 23:00-ish EST; 2024-06-15T12:00Z is 08:00 EDT"
    );
    assert_eq!(
        (ansi_types, ansi_columns),
        (spark_types, spark_columns),
        "the door must not change the answer — the session zone belongs to the session"
    );

    let tokyo = spark_extended_session_at(TOKYO);
    let (_, tokyo_columns) = int_columns_through(&tokyo, &ansi, EXTRACT_SQL).await;
    assert_eq!(
        tokyo_columns,
        vec![vec![2024, 2024], vec![13, 21]],
        "east of UTC, through the ANSI door, with no Spark dialect involved anywhere"
    );
}

/// The honest negative: a session with NO extension is stock DataFusion, whose `date_part` reads
/// the array's own zone and knows nothing about `spark.sql.session.timeZone`. Recorded so the
/// boundary is a stated property of the extension-less profile rather than an unexplained gap.
#[tokio::test]
async fn a_native_session_without_the_spark_extension_reads_the_stored_zone() {
    let ansi: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let native = ReparkSession::builder()
        .with_sql_dialect(Arc::clone(&ansi))
        .config(repark_core::SESSION_TIME_ZONE_KEY, NEW_YORK)
        .build()
        .expect("a native session");
    register_instants(&native);
    assert_eq!(
        native.session_time_zone().id(),
        NEW_YORK,
        "the session still CARRIES the zone — it is the function layer that is absent"
    );
    let (_, columns) = int_columns_through(
        &native,
        &ansi,
        "SELECT date_part('hour', ts) AS h FROM t ORDER BY ts",
    )
    .await;
    assert_eq!(
        columns,
        vec![vec![4, 12]],
        "stock DataFusion extracts in the array's stored (UTC) zone; Spark semantics arrive with \
         the Spark extension, which is what installs the zone-aware extractors"
    );
}
