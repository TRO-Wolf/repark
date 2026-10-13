use datafusion::prelude::SessionConfig;
use iceberg::spec::{PrimitiveType, Type};
use repark_core::SqlDialect;
use repark_functions::cardinality::{ReparkSqlSettings, with_repark_sql_config};

use super::create::{Door, door_with_config};

const SETTING: &str = "build the session with `repark.sql.allowImplicitTimestampNanos` = true in \
     its configuration (the key is read once, when the session is built)";
const MICRO: &str = "For microseconds write CAST(<expression> AS TIMESTAMP(6)).";
const CREATE_V3: &str = "Create the table WITH (format_version = 3).";
const REPLACE_V3: &str = "Replace the table WITH (format_version = 3), or upgrade it first with \
     ALTER TABLE … SET PROPERTIES (format_version = 3).";
const PROPERTY: &str = "The table must be format version 3: table property format_version = 3.";
const V3: &str = "WITH (format_version = 3)";
const UPGRADE: &str = "SET PROPERTIES (format_version = 3)";

async fn door(setting: bool) -> Door {
    let settings = ReparkSqlSettings {
        allow_create_format_version_3: true,
        allow_implicit_timestamp_nanos: setting,
        ..ReparkSqlSettings::default()
    };
    let config = SessionConfig::new().with_information_schema(true);
    let door = door_with_config(with_repark_sql_config(config, settings)).await;
    crate::AnsiDialect.on_session_built(&door.ctx);
    let location = format!("{}/sales", door.warehouse);
    door.ok(&format!(
        "CREATE SCHEMA ice.sales WITH (location = '{location}')"
    ))
    .await;
    door.ok(&format!(
        "CREATE TABLE ice.sales.seed (ts timestamp_ns, tz timestamptz_ns) {V3}"
    ))
    .await;
    door
}

async fn stored(door: &Door, table: &str) -> Vec<Type> {
    let loaded = door.table("sales", table).await;
    let schema = loaded.metadata().current_schema();
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.field_type.as_ref().clone())
        .collect()
}

fn primitive(kind: PrimitiveType) -> Vec<Type> {
    vec![Type::Primitive(kind)]
}

fn nano_of(column: &str) -> (&'static str, PrimitiveType) {
    if column == "tz" {
        ("timestamptz_ns", PrimitiveType::TimestamptzNs)
    } else {
        ("timestamp_ns", PrimitiveType::TimestampNs)
    }
}

fn not_requested(form: &str, column: &str, below: Option<&str>) -> String {
    let below = below.map_or_else(String::new, |remedy| {
        format!(" A nanosecond column also needs a format version 3 table. {remedy}")
    });
    format!(
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] {form} would create column \
         `{column}` as {nano}, a type Spark cannot read, and the statement does not ask for it. \
         To ask, {SETTING}.{below} {MICRO}",
        nano = nano_of(column).0
    )
}

fn requires_v3(column: &str, remedy: &str) -> String {
    format!(
        "Error during planning: [TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for {column}: \
         {nano} is not supported until v3. {remedy}",
        nano = nano_of(column).0
    )
}

#[tokio::test]
async fn every_remedy_an_unasked_refusal_names_works_when_followed() {
    for column in ["ts", "tz"] {
        let nanos = primitive(nano_of(column).1);
        let plain = format!("SELECT {column} FROM ice.sales.seed");
        let micro =
            format!("SELECT CAST({column} AS TIMESTAMP(6)) AS {column} FROM ice.sales.seed");

        let refusing = door(false).await;
        let asking = door(true).await;
        let at_v3 = format!("CREATE TABLE ice.sales.a {V3} AS {plain}");
        assert_eq!(
            refusing.err(&at_v3).await,
            not_requested("CTAS", column, None)
        );
        assert!(!refusing.table_exists("sales", "a").await);
        asking.ok(&at_v3).await;
        assert_eq!(stored(&asking, "a").await, nanos);
        refusing
            .ok(&format!("CREATE TABLE ice.sales.a {V3} AS {micro}"))
            .await;
        assert_eq!(
            stored(&refusing, "a").await,
            primitive(PrimitiveType::Timestamp)
        );

        let default_version = format!("CREATE TABLE ice.sales.b AS {plain}");
        assert_eq!(
            refusing.err(&default_version).await,
            not_requested("CTAS", column, Some(CREATE_V3))
        );
        assert_eq!(
            asking.err(&default_version).await,
            requires_v3(column, CREATE_V3)
        );
        asking
            .ok(&format!("CREATE TABLE ice.sales.b {V3} AS {plain}"))
            .await;
        assert_eq!(stored(&asking, "b").await, nanos);
        refusing
            .ok(&format!("CREATE TABLE ice.sales.b AS {micro}"))
            .await;

        for session in [&refusing, &asking] {
            session.ok("CREATE TABLE ice.sales.c (id INT)").await;
            session.ok("CREATE TABLE ice.sales.d (id INT)").await;
        }
        let replace = |table: &str, properties: &str| {
            format!("CREATE OR REPLACE TABLE ice.sales.{table} {properties} AS {plain}")
        };
        assert_eq!(
            refusing.err(&replace("c", "")).await,
            not_requested("RTAS", column, Some(REPLACE_V3))
        );
        assert_eq!(
            asking.err(&replace("c", "")).await,
            requires_v3(column, REPLACE_V3)
        );
        asking.ok(&replace("c", V3)).await;
        assert_eq!(stored(&asking, "c").await, nanos);
        asking
            .ok(&format!("ALTER TABLE ice.sales.d {UPGRADE}"))
            .await;
        asking.ok(&replace("d", "")).await;
        assert_eq!(stored(&asking, "d").await, nanos);
    }
}

#[tokio::test]
async fn every_remedy_a_named_type_below_v3_names_works_when_followed() {
    let session = door(false).await;
    let ddl = "CREATE TABLE ice.sales.below (id INT, ts timestamp_ns)";
    assert_eq!(session.err(ddl).await, requires_v3("ts", CREATE_V3));
    assert!(!session.table_exists("sales", "below").await);
    session.ok(&format!("{ddl} {V3}")).await;
    session.ok("CREATE TABLE ice.sales.old (id INT)").await;
    let add = "ALTER TABLE ice.sales.old ADD COLUMN ts timestamp_ns";
    let refused = session.err(add).await;
    assert_eq!(refused, requires_v3("ts", PROPERTY));
    session
        .ok(&format!("ALTER TABLE ice.sales.old {UPGRADE}"))
        .await;
    session.ok(add).await;
    assert_eq!(
        stored(&session, "old").await[1],
        Type::Primitive(PrimitiveType::TimestampNs)
    );
    let kernel = "SELECT __repark_cast_timestamp_ns__('2026-01-02 03:04:05.123456789') AS ts";
    assert_eq!(
        session
            .err(&format!("CREATE TABLE ice.sales.k AS {kernel}"))
            .await,
        requires_v3("ts", CREATE_V3)
    );
    session
        .ok(&format!("CREATE TABLE ice.sales.k {V3} AS {kernel}"))
        .await;
    assert_eq!(
        stored(&session, "k").await,
        primitive(PrimitiveType::TimestampNs)
    );
}

#[tokio::test]
async fn a_kept_column_passes_a_native_rtas_and_a_retyped_one_is_new() {
    let session = door(false).await;
    session
        .ok(&format!(
            "CREATE TABLE ice.sales.kept (ts timestamp_ns) {V3}"
        ))
        .await;
    session
        .ok(&format!(
            "CREATE TABLE ice.sales.other (ts TIMESTAMP(6)) {V3}"
        ))
        .await;
    let replace = "CREATE OR REPLACE TABLE ice.sales.{} AS SELECT ts FROM ice.sales.seed";
    session.ok(&replace.replace("{}", "kept")).await;
    assert_eq!(
        stored(&session, "kept").await,
        primitive(PrimitiveType::TimestampNs)
    );
    assert_eq!(
        session.err(&replace.replace("{}", "other")).await,
        not_requested("RTAS", "ts", None)
    );
}

#[tokio::test]
async fn a_plain_timestamp_expression_in_a_native_ctas_at_v3_is_refused() {
    let session = door(false).await;
    for select in [
        "SELECT TIMESTAMP '2026-01-02 03:04:05' AS ts",
        "SELECT now() AS ts",
        "SELECT ts FROM ice.sales.seed",
    ] {
        let sql = format!("CREATE TABLE ice.sales.plain {V3} AS {select}");
        assert_eq!(
            session.err(&sql).await,
            not_requested("CTAS", "ts", None),
            "{sql}"
        );
        assert!(!session.table_exists("sales", "plain").await);
    }
}

#[tokio::test]
async fn plain_timestamp_in_native_add_column_and_nested_ddl_still_stores_nanoseconds_as_main() {
    let session = door(false).await;
    session
        .ok(&format!("CREATE TABLE ice.sales.t (id INT) {V3}"))
        .await;
    session
        .ok("ALTER TABLE ice.sales.t ADD COLUMN ts TIMESTAMP")
        .await;
    assert_eq!(
        stored(&session, "t").await[1],
        Type::Primitive(PrimitiveType::TimestampNs)
    );
    for (table, column) in [("a", "a ARRAY<TIMESTAMP>"), ("s", "st STRUCT<v TIMESTAMP>")] {
        session
            .ok(&format!(
                "CREATE TABLE ice.sales.{table} (id INT, {column}) {V3}"
            ))
            .await;
        let nested = format!("{:?}", stored(&session, table).await[1]);
        assert!(nested.contains("TimestampNs"), "{nested}");
    }
    let top_level = session
        .err(&format!(
            "CREATE TABLE ice.sales.top (id INT, ts TIMESTAMP) {V3}"
        ))
        .await;
    assert!(
        top_level.contains("Declare TIMESTAMP(6) or Iceberg timestamp_ns"),
        "{top_level}"
    );
}
