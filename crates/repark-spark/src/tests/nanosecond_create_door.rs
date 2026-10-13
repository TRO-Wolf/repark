use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use repark_functions::cardinality::ReparkSqlSettings;

use super::super::*;
use super::common::*;

const SEED: i64 = 1_767_323_045_123_456_789;
const V3: &str = "TBLPROPERTIES ('format-version' = '3')";
const EVOLVING: &str =
    "TBLPROPERTIES ('format-version' = '3', 'write.spark.accept-any-schema' = 'true')";
const COLUMNS: [(&str, &str, &str); 2] = [
    ("c", "timestamp_ns", "TIMESTAMP_NTZ"),
    ("cz", "timestamptz_ns", "TIMESTAMP"),
];

async fn door(warehouse: &TempDir, setting: bool) -> (SessionContext, CatalogRegistry) {
    let settings = ReparkSqlSettings {
        allow_create_format_version_3: true,
        allow_implicit_timestamp_nanos: setting,
        ..ReparkSqlSettings::default()
    };
    let (ctx, catalogs) = setup_with_sql_settings(warehouse, true, settings).await;
    for function in repark_functions::instant_ts::functions() {
        ctx.register_udf(function.as_ref().clone());
    }
    ctx.state_ref()
        .write()
        .config_mut()
        .options_mut()
        .extensions
        .insert(repark_functions::merge_schema::MergeSchemaConfig { enabled: true });
    for statement in [
        format!(
            "CREATE TABLE ice.sales.seed (id INT, c timestamp_ns, cz timestamptz_ns) \
             USING iceberg {V3}"
        ),
        "INSERT INTO ice.sales.seed VALUES \
         (1, CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns), \
         CAST('2026-01-02 03:04:05.123456789+00:00' AS timestamptz_ns)), (2, NULL, NULL)"
            .to_string(),
        "CREATE TABLE ice.sales.micro (id INT, c TIMESTAMP_NTZ, cz TIMESTAMP) USING iceberg"
            .to_string(),
        "INSERT INTO ice.sales.micro VALUES \
         (1, TIMESTAMP_NTZ '2026-01-02 03:04:05.123456', \
         TIMESTAMP '2026-01-02 03:04:05.123456+00:00'), (2, NULL, NULL)"
            .to_string(),
    ] {
        run(&ctx, &catalogs, &statement).await;
    }
    (ctx, catalogs)
}

fn select(column: &str, written: bool) -> String {
    let (_, nano, _) = COLUMNS
        .iter()
        .find(|(name, _, _)| *name == column)
        .expect("a matrix column");
    if written {
        format!("SELECT id, CAST({column} AS {nano}) AS {column} FROM ice.sales.seed")
    } else {
        format!("SELECT id, {column} FROM ice.sales.seed")
    }
}

fn prepare(door: &str) -> Option<String> {
    match door {
        "replace" => Some(format!(
            "CREATE TABLE ice.sales.t (id INT, keepcol INT) USING iceberg {V3}"
        )),
        "merge_schema" => Some(format!(
            "CREATE TABLE ice.sales.t (id INT) USING iceberg {EVOLVING}"
        )),
        "merge_evolution" => Some(format!(
            "CREATE TABLE ice.sales.t (id INT) USING iceberg {V3}"
        )),
        _ => None,
    }
}

fn statement(door: &str, select: &str) -> String {
    match door {
        "ctas" => format!("CREATE TABLE ice.sales.t USING iceberg {V3} AS {select}"),
        "create_or_replace" => {
            format!("CREATE OR REPLACE TABLE ice.sales.t USING iceberg {V3} AS {select}")
        }
        "replace" => format!("REPLACE TABLE ice.sales.t USING iceberg AS {select}"),
        "merge_schema" => format!("INSERT INTO ice.sales.t {select}"),
        _ => format!(
            "MERGE WITH SCHEMA EVOLUTION INTO ice.sales.t t USING ({select}) s ON t.id = s.id \
             WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        ),
    }
}

const DOORS: [(&str, &str); 5] = [
    ("ctas", "CTAS"),
    ("create_or_replace", "RTAS"),
    ("replace", "RTAS"),
    ("merge_schema", "mergeSchema append"),
    ("merge_evolution", "MERGE WITH SCHEMA EVOLUTION"),
];

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(), String> {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame.collect().await.map(|_| ()).map_err(|e| e.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn stored_types(catalogs: &CatalogRegistry) -> Option<Vec<(String, String)>> {
    let ident = TableIdent::new(NamespaceIdent::new("sales".into()), "t".into());
    let catalog = catalog_handle(catalogs, "ice").unwrap();
    if !catalog.table_exists(&ident).await.unwrap() {
        return None;
    }
    let table = catalog.load_table(&ident).await.unwrap();
    let schema = table.metadata().current_schema();
    Some(
        schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| (field.name.clone(), field.field_type.to_string()))
            .collect(),
    )
}

async fn stored_ticks(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<Option<i64>> {
    let batches = execute(ctx, catalogs, "SELECT * FROM ice.sales.t ORDER BY id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let ints = cast(batch.column(batch.num_columns() - 1), &DataType::Int64).unwrap();
            ints.as_primitive::<Int64Type>().iter().collect::<Vec<_>>()
        })
        .collect()
}

fn not_requested(form: &str, name: &str, nano: &str, micro: &str) -> String {
    format!(
        "Error during planning: [TIMESTAMP_NANOS_NOT_REQUESTED] {form} would create column \
         `{name}` as {nano}, a type Spark cannot read, and the statement does not ask for it. \
         Ask with CAST(<expression> AS {nano}) on the column (SQL or selectExpr), or set \
         `repark.sql.allowImplicitTimestampNanos` = true on the session. For microseconds write \
         CAST(<expression> AS {micro})."
    )
}

#[tokio::test]
async fn an_unasked_nanosecond_column_is_refused_on_every_sql_door() {
    for (door_name, form) in DOORS {
        for (column, nano, micro) in COLUMNS {
            let warehouse = TempDir::new().unwrap();
            let (ctx, catalogs) = door(&warehouse, false).await;
            if let Some(create) = prepare(door_name) {
                run(&ctx, &catalogs, &create).await;
            }
            let before = stored_types(&catalogs).await;
            let files = count_objects(warehouse.path());
            let sql = statement(door_name, &select(column, false));
            assert_eq!(
                outcome(&ctx, &catalogs, &sql).await,
                Err(not_requested(form, column, nano, micro)),
                "{door_name} {column}"
            );
            assert_eq!(stored_types(&catalogs).await, before, "{door_name}");
            assert_eq!(count_objects(warehouse.path()), files, "{door_name}");
        }
    }
}

#[tokio::test]
async fn a_written_cast_or_the_setting_creates_the_column_on_every_sql_door() {
    for (door_name, _) in DOORS {
        for (column, nano, _) in COLUMNS {
            for (setting, written) in [(false, true), (true, false)] {
                let warehouse = TempDir::new().unwrap();
                let (ctx, catalogs) = door(&warehouse, setting).await;
                if let Some(create) = prepare(door_name) {
                    run(&ctx, &catalogs, &create).await;
                }
                let sql = statement(door_name, &select(column, written));
                assert_eq!(outcome(&ctx, &catalogs, &sql).await, Ok(()), "{sql}");
                assert_eq!(
                    stored_types(&catalogs).await,
                    Some(vec![
                        ("id".to_string(), "int".to_string()),
                        (column.to_string(), nano.to_string()),
                    ]),
                    "{sql}"
                );
                assert_eq!(stored_ticks(&ctx, &catalogs).await, vec![Some(SEED), None]);
            }
        }
    }
}

#[tokio::test]
async fn a_kept_nanosecond_column_survives_rtas_and_evolution_without_an_ask() {
    for (door_name, _) in DOORS.iter().skip(1) {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = door(&warehouse, false).await;
        let properties = if *door_name == "merge_schema" {
            EVOLVING
        } else {
            V3
        };
        let create =
            format!("CREATE TABLE ice.sales.t (id INT, C timestamp_ns) USING iceberg {properties}");
        run(&ctx, &catalogs, &create).await;
        let sql = statement(door_name, &select("c", false));
        assert_eq!(outcome(&ctx, &catalogs, &sql).await, Ok(()), "{sql}");
        assert_eq!(stored_ticks(&ctx, &catalogs).await, vec![Some(SEED), None]);
        if matches!(*door_name, "create_or_replace" | "replace") {
            let retyped = statement(door_name, "SELECT id, cz AS c FROM ice.sales.seed");
            assert_eq!(
                outcome(&ctx, &catalogs, &retyped).await,
                Err(not_requested("RTAS", "c", "timestamptz_ns", "TIMESTAMP"))
            );
        }
    }
}

#[tokio::test]
async fn statements_with_no_nanosecond_output_are_not_refused() {
    for (door_name, _) in DOORS {
        let warehouse = TempDir::new().unwrap();
        let (ctx, catalogs) = door(&warehouse, false).await;
        if let Some(create) = prepare(door_name) {
            run(&ctx, &catalogs, &create).await;
        }
        let sql = statement(door_name, "SELECT id, c FROM ice.sales.micro");
        assert_eq!(outcome(&ctx, &catalogs, &sql).await, Ok(()), "{sql}");
        assert_eq!(
            stored_types(&catalogs).await,
            Some(vec![
                ("id".to_string(), "int".to_string()),
                ("c".to_string(), "timestamp".to_string()),
            ])
        );
    }
}

#[tokio::test]
async fn format_version_3_alone_is_not_an_ask_and_an_unasked_column_below_v3_gets_the_same_text() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&warehouse, false).await;
    let plain = select("c", false);
    for sql in [
        format!("CREATE TABLE ice.sales.t USING iceberg {V3} AS {plain}"),
        format!("CREATE TABLE ice.sales.t USING iceberg AS {plain}"),
    ] {
        assert_eq!(
            outcome(&ctx, &catalogs, &sql).await,
            Err(not_requested("CTAS", "c", "timestamp_ns", "TIMESTAMP_NTZ"))
        );
        assert_eq!(stored_types(&catalogs).await, None);
    }
}

#[tokio::test]
async fn ddl_and_add_column_naming_the_type_still_create_it() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&warehouse, false).await;
    for sql in [
        format!("CREATE TABLE ice.sales.t (id INT, c timestamp_ns) USING iceberg {V3}"),
        "ALTER TABLE ice.sales.t ADD COLUMN cz timestamptz_ns".to_string(),
    ] {
        assert_eq!(outcome(&ctx, &catalogs, &sql).await, Ok(()), "{sql}");
    }
    assert_eq!(
        stored_types(&catalogs).await,
        Some(vec![
            ("id".to_string(), "int".to_string()),
            ("c".to_string(), "timestamp_ns".to_string()),
            ("cz".to_string(), "timestamptz_ns".to_string()),
        ])
    );
}

#[tokio::test]
async fn a_nanosecond_type_the_statement_asks_for_below_v3_names_the_format_remedy() {
    let below = |name: &str, nano: &str| {
        format!(
            "Error during planning: [TIMESTAMP_NANOS_REQUIRES_FORMAT_V3] Invalid type for \
             {name}: {nano} is not supported until v3. Create the table with TBLPROPERTIES \
             ('format-version'='3') (writeTo: tableProperty) and \
             `repark.sql.allowCreateFormatVersion3` = true."
        )
    };
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = door(&warehouse, false).await;
    let cast = select("cz", true);
    for (sql, name, nano) in [
        (
            format!("CREATE TABLE ice.sales.t USING iceberg AS {cast}"),
            "cz",
            "timestamptz_ns",
        ),
        (
            "CREATE TABLE ice.sales.t (id INT, c timestamp_ns) USING iceberg".to_string(),
            "c",
            "timestamp_ns",
        ),
    ] {
        assert_eq!(
            outcome(&ctx, &catalogs, &sql).await,
            Err(below(name, nano)),
            "{sql}"
        );
        assert_eq!(stored_types(&catalogs).await, None);
    }
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id INT) USING iceberg",
    )
    .await;
    assert_eq!(
        outcome(
            &ctx,
            &catalogs,
            "ALTER TABLE ice.sales.t ADD COLUMN c2 timestamp_ns"
        )
        .await,
        Err(below("c2", "timestamp_ns"))
    );
}
