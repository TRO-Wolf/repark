/// `CREATE NAMESPACE` / `DROP NAMESPACE` against the catalog, with `IF [NOT] EXISTS` idempotency.
use super::super::*;
use super::common::*;
use datafusion::arrow::array::AsArray;

async fn describe_namespace_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(String, String)> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let names = batch.column(0).as_string::<i32>();
            let values = batch.column(1).as_string::<i32>();
            (0..batch.num_rows())
                .map(move |row| (names.value(row).to_string(), values.value(row).to_string()))
        })
        .collect()
}

#[tokio::test]
async fn create_and_drop_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let ns = NamespaceIdent::new("analytics".to_string());

    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.analytics")
        .await
        .unwrap();
    assert!(catalogs["ice"].namespace_exists(&ns).await.unwrap());
    // IF NOT EXISTS is idempotent.
    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE IF NOT EXISTS ice.analytics",
    )
    .await
    .unwrap();

    execute(&ctx, &catalogs, "DROP NAMESPACE ice.analytics")
        .await
        .unwrap();
    assert!(!catalogs["ice"].namespace_exists(&ns).await.unwrap());
    // IF EXISTS on the now-missing namespace is a no-op.
    execute(&ctx, &catalogs, "DROP NAMESPACE IF EXISTS ice.analytics")
        .await
        .unwrap();
}

#[tokio::test]
async fn alter_namespace_properties_update_catalog_and_describe() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.nsa")
        .await
        .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER NAMESPACE ice.nsa SET DBPROPERTIES ('b' = '2')",
    )
    .await
    .unwrap();
    let properties = catalogs["ice"]
        .get_namespace(&NamespaceIdent::new("nsa".to_string()))
        .await
        .unwrap()
        .properties()
        .clone();
    assert_eq!(properties.get("b").map(String::as_str), Some("2"));
    assert_eq!(
        describe_namespace_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.nsa").await,
        vec![
            ("Catalog Name".to_string(), "ice".to_string()),
            ("Namespace Name".to_string(), "nsa".to_string()),
            ("Properties".to_string(), "((b,2))".to_string()),
        ]
    );
    execute(
        &ctx,
        &catalogs,
        "ALTER NAMESPACE ice.nsa SET PROPERTIES ('z' = '9', 'a' = '1')",
    )
    .await
    .unwrap();
    assert_eq!(
        describe_namespace_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.nsa")
            .await
            .last(),
        Some(&(
            "Properties".to_string(),
            "((a,1), (b,2), (z,9))".to_string()
        ))
    );
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.nsorder")
        .await
        .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER NAMESPACE ice.nsorder SET PROPERTIES ('z' = '9', 'a' = '1')",
    )
    .await
    .unwrap();
    assert_eq!(
        describe_namespace_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.nsorder")
            .await
            .last(),
        Some(&("Properties".to_string(), "((a,1), (z,9))".to_string()))
    );
}

async fn namespace_properties(
    catalogs: &CatalogRegistry,
    namespace: &str,
) -> HashMap<String, String> {
    catalogs["ice"]
        .get_namespace(&NamespaceIdent::new(namespace.to_string()))
        .await
        .unwrap()
        .properties()
        .clone()
}

#[tokio::test]
async fn alter_namespace_on_a_missing_namespace_is_schema_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for sql in [
        "ALTER NAMESPACE ice.nope SET DBPROPERTIES ('b' = '2')",
        "ALTER NAMESPACE ice.nope SET PROPERTIES ('b' = '2')",
        "ALTER DATABASE ice.nope SET DBPROPERTIES ('b' = '2')",
    ] {
        let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
        assert!(matches!(error, DataFusionError::Plan(_)), "{sql}: {error}");
        assert_eq!(
            error.to_string(),
            "Error during planning: [SCHEMA_NOT_FOUND] The schema `nope` cannot be found. \
             Verify the spelling and correctness of the schema and catalog.\nIf you did not \
             qualify the name with a catalog, verify the current_schema() output, or qualify the \
             name with the correct catalog.\nTo tolerate the error on drop use DROP SCHEMA IF \
             EXISTS. SQLSTATE: 42704",
            "{sql}"
        );
    }
}

#[tokio::test]
async fn alter_namespace_refuses_the_properties_spark_refuses() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.nsr")
        .await
        .unwrap();
    let before = namespace_properties(&catalogs, "nsr").await;
    let location = "[UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY] The feature is not supported: \
                    location is a reserved namespace property, please use the LOCATION clause to \
                    specify it. SQLSTATE: 0A000";
    let owner = "[UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY] The feature is not supported: owner \
                 is a reserved namespace property, it will be set to the current user. SQLSTATE: \
                 0A000";
    let duplicate = "[DUPLICATE_KEY] Found duplicate keys `a`. SQLSTATE: 23505";
    let near = |token: &str| {
        format!("[PARSE_SYNTAX_ERROR] Syntax error at or near '{token}'. SQLSTATE: 42601")
    };
    let cases = [
        (
            "ice.nsr SET DBPROPERTIES ('location' = '/x')",
            location.to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('z' = '1', 'location' = '/x')",
            location.to_string(),
        ),
        (
            "ice.nope SET DBPROPERTIES ('location' = '/x')",
            location.to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('owner' = 'bob')",
            owner.to_string(),
        ),
        (
            "ice.nsr SET PROPERTIES ('owner' = 'bob')",
            owner.to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('owner' = '1', 'location' = '2')",
            owner.to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('a' = '1', 'a' = '2')",
            duplicate.to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES (a = '1', 'a' = '2')",
            duplicate.to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('location' = '1', 'a' = '1', 'a' = '2')",
            duplicate.to_string(),
        ),
        ("ice.nsr SET DBPROPERTIES ()", near(")")),
        ("ice.nsr SET DBPROPERTIES ('w' = foo)", near("foo")),
        ("ice.nsr SET DBPROPERTIES (1 = 'v')", near("1")),
        ("ice.nsr SET DBPROPERTIES ('neg' = -1)", near("-")),
        ("ice.nsr SET DBPROPERTIES ('nv' =)", near(")")),
        (
            "ice.nsr SET DBPROPERTIES ('k' = 'v'",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601"
                .to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('k2' = 'v') extra",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'extra': extra input 'extra'. \
             SQLSTATE: 42601"
                .to_string(),
        ),
        (
            "ice.nsr SET DBPROPERTIES ('nv')",
            "SQL error: ParserError(\"Operation not allowed: Values must be specified for \
             key(s): [nv].\")"
                .to_string(),
        ),
    ];
    for (tail, expected) in cases {
        let sql = format!("ALTER NAMESPACE {tail}");
        let error = execute(&ctx, &catalogs, &sql).await.expect_err(&sql);
        let mapped = repark_core::engine_err(error);
        let repark_common::Error::Parse(message) = &mapped else {
            panic!("{sql}: expected a Parse error, got {mapped:?}");
        };
        assert_eq!(message, &expected, "{sql}");
    }
    assert_eq!(namespace_properties(&catalogs, "nsr").await, before);
}

#[tokio::test]
async fn alter_namespace_accepts_the_property_shapes_spark_accepts() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.nsp")
        .await
        .unwrap();
    for tail in [
        "SET DBPROPERTIES (wk = 'v')",
        "SET DBPROPERTIES (a.b = 'v')",
        "SET DBPROPERTIES (UpK = 'v')",
        "SET DBPROPERTIES ('n' = 5, 'dv' = 1.5)",
        "SET DBPROPERTIES ('t' = true, 'tu' = TRUE)",
        "SET DBPROPERTIES (\"dq\" = \"v\")",
        "SET DBPROPERTIES (nk 'v')",
        "SET DBPROPERTIES ('LOCATION' = '/x')",
        "SET DBPROPERTIES ('comment' = 'c')",
    ] {
        let sql = format!("ALTER NAMESPACE ice.nsp {tail}");
        execute(&ctx, &catalogs, &sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    assert_eq!(
        describe_namespace_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.nsp").await,
        vec![
            ("Catalog Name".to_string(), "ice".to_string()),
            ("Namespace Name".to_string(), "nsp".to_string()),
            ("Comment".to_string(), "c".to_string()),
            (
                "Properties".to_string(),
                "((LOCATION,/x), (UpK,v), (a.b,v), (dq,v), (dv,1.5), (n,5), (nk,v), (t,true), \
                 (tu,true), (wk,v))"
                    .to_string()
            ),
        ]
    );
}

/// WG-5 C-1: CREATE NAMESPACE LOCATION on a strict catalog lets a later CTAS land under that path.
#[tokio::test]
async fn sql_create_namespace_location_lets_ctas_land_under_it() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/silver_location");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();

    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.silver.orders").await,
        3
    );
    assert!(
        count_parquet_files(std::path::Path::new(&location)) > 0,
        "CTAS data must physically land under the SQL `LOCATION` `{location}`"
    );
}

/// SQL `CREATE NAMESPACE … LOCATION '/x'` stores equal `location` and `location_uri` keys.
#[tokio::test]
async fn sql_create_namespace_location_stores_both_location_keys() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/dual_write");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();

    let props = namespace_props(&catalogs, "silver").await;
    assert_eq!(
        props.get("location").map(String::as_str),
        Some(location.as_str()),
        "the SQL LOCATION must be stored under `location`"
    );
    assert_eq!(
        props.get("location_uri").map(String::as_str),
        Some(location.as_str()),
        "the SQL LOCATION must ALSO be mirrored onto `location_uri` (the U2 dual-write)"
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert!(count_parquet_files(std::path::Path::new(&location)) > 0);
}

/// U2-P7: an explicitly-set `location_uri` is NEVER overwritten by the mirror.
#[tokio::test]
async fn sql_create_namespace_explicit_location_uri_is_never_overwritten() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location_a = format!("{warehouse}/explicit_location");
    let location_b = format!("{warehouse}/explicit_location_uri");

    execute(
        &ctx,
        &catalogs,
        &format!(
            "CREATE NAMESPACE glue_like.silver LOCATION '{location_a}' \
                 WITH DBPROPERTIES ('location_uri' = '{location_b}')"
        ),
    )
    .await
    .unwrap();
    let props = namespace_props(&catalogs, "silver").await;
    assert_eq!(
        props.get("location").map(String::as_str),
        Some(location_a.as_str())
    );
    assert_eq!(
        props.get("location_uri").map(String::as_str),
        Some(location_b.as_str()),
        "an explicitly-set `location_uri` must never be overwritten by the mirror"
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert!(
        count_parquet_files(std::path::Path::new(&location_a)) > 0,
        "CTAS must land under `location` (read precedence) even with a different location_uri"
    );

    // Unidirectional: a location_uri-only create is stored exactly as written.
    let location_c = format!("{warehouse}/uri_only");
    execute(
        &ctx,
        &catalogs,
        &format!(
            "CREATE NAMESPACE glue_like.gold WITH DBPROPERTIES ('location_uri' = '{location_c}')"
        ),
    )
    .await
    .unwrap();
    let gold_props = namespace_props(&catalogs, "gold").await;
    assert_eq!(
        gold_props.get("location_uri").map(String::as_str),
        Some(location_c.as_str())
    );
    assert!(
        !gold_props.contains_key("location"),
        "the mirror must NOT synthesize `location` from an explicit `location_uri`"
    );
}

/// WG-5 C-2: `WITH DBPROPERTIES` round-trips into the namespace metadata.
#[tokio::test]
async fn sql_create_namespace_with_dbproperties_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/dbprops_location");

    execute(
        &ctx,
        &catalogs,
        &format!(
            "CREATE NAMESPACE glue_like.silver \
                 WITH DBPROPERTIES ('location' = '{location}', 'owner' = 'example-team')"
        ),
    )
    .await
    .unwrap();

    let props = namespace_props(&catalogs, "silver").await;
    assert_eq!(props.get("owner").map(String::as_str), Some("example-team"));
    assert_eq!(
        props.get("location").map(String::as_str),
        Some(location.as_str())
    );

    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.silver.orders").await,
        3
    );
    assert!(count_parquet_files(std::path::Path::new(&location)) > 0);
}

/// WG-5 C-2: Spark accepts `WITH PROPERTIES` as well as `WITH DBPROPERTIES`.
#[tokio::test]
async fn sql_create_namespace_with_properties_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/props_location");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver WITH PROPERTIES ('location' = '{location}')"),
    )
    .await
    .unwrap();

    assert_eq!(
        namespace_props(&catalogs, "silver")
            .await
            .get("location")
            .map(String::as_str),
        Some(location.as_str())
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.silver.orders").await,
        3
    );
    assert!(count_parquet_files(std::path::Path::new(&location)) > 0);
}

/// WG-5 C-3 + G-6 Q1: `IF NOT EXISTS` with the SAME location is idempotent.
#[tokio::test]
async fn sql_create_namespace_if_not_exists_is_idempotent() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/idempotent_location");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE IF NOT EXISTS glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();

    assert_eq!(
        namespace_props(&catalogs, "silver")
            .await
            .get("location")
            .map(String::as_str),
        Some(location.as_str()),
        "IF NOT EXISTS must not overwrite the existing namespace's location"
    );
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.silver.orders").await,
        3
    );
    assert!(
        count_parquet_files(std::path::Path::new(&location)) > 0,
        "the CTAS must land under the ORIGINAL location, not the IF-NOT-EXISTS no-op's"
    );
}

/// G-6 Q1 create-new: `CREATE NAMESPACE IF NOT EXISTS` on a missing name creates it.
#[tokio::test]
async fn sql_create_namespace_if_not_exists_create_new() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE IF NOT EXISTS ice.fresh_ns",
    )
    .await
    .unwrap();
    assert!(
        catalogs["ice"]
            .namespace_exists(&NamespaceIdent::new("fresh_ns".to_string()))
            .await
            .unwrap(),
        "IF NOT EXISTS on a missing namespace must create it"
    );
}

/// G-6 Q1 re-create-same: `IF NOT EXISTS` with the same LOCATION is a no-op.
#[tokio::test]
async fn sql_create_namespace_if_not_exists_same_location_is_idempotent() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/same_location");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE IF NOT EXISTS glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();
    assert_eq!(
        namespace_props(&catalogs, "silver")
            .await
            .get("location")
            .map(String::as_str),
        Some(location.as_str())
    );
}

/// G-6 Q1 re-create-conflicting: `IF NOT EXISTS … LOCATION`.
#[tokio::test]
async fn sql_create_namespace_if_not_exists_conflicting_location_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let existing = format!("{warehouse}/existing");
    let requested = format!("{warehouse}/requested");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{existing}'"),
    )
    .await
    .unwrap();
    let error = execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE IF NOT EXISTS glue_like.silver LOCATION '{requested}'"),
    )
    .await
    .expect_err("contradictory IF NOT EXISTS LOCATION must fail loud");
    let message = error.to_string();
    assert!(
        message.contains(&existing),
        "must name the existing path: {message}"
    );
    assert!(
        message.contains(&requested),
        "must name the requested path: {message}"
    );
    assert_eq!(
        namespace_props(&catalogs, "silver")
            .await
            .get("location")
            .map(String::as_str),
        Some(existing.as_str()),
        "a refused IF NOT EXISTS must not rewrite the stored location"
    );
}

/// G-6 Q1 re-create-without-location: `IF NOT EXISTS` with no LOCATION adopts the existing ns.
#[tokio::test]
async fn sql_create_namespace_if_not_exists_without_location_is_idempotent() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/kept_location");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE IF NOT EXISTS glue_like.silver",
    )
    .await
    .unwrap();
    assert_eq!(
        namespace_props(&catalogs, "silver")
            .await
            .get("location")
            .map(String::as_str),
        Some(location.as_str()),
        "IF NOT EXISTS without LOCATION must keep the stored location"
    );
}

/// `CREATE DATABASE` is a synonym for `CREATE NAMESPACE` and creates an Iceberg namespace.
#[tokio::test]
async fn sql_create_database_synonym_creates_namespace() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    execute(&ctx, &catalogs, "CREATE DATABASE ice.warehouse_db")
        .await
        .unwrap();
    assert!(
        catalogs["ice"]
            .namespace_exists(&NamespaceIdent::new("warehouse_db".to_string()))
            .await
            .unwrap()
    );
}

/// WG-5 C-7: `CREATE SCHEMA` is a synonym too, and carries `LOCATION` like `CREATE NAMESPACE`.
#[tokio::test]
async fn sql_create_schema_synonym_with_location_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, warehouse) = setup_strict_catalog(&wh).await;
    let location = format!("{warehouse}/schema_syn_location");

    execute(
        &ctx,
        &catalogs,
        &format!("CREATE SCHEMA glue_like.silver LOCATION '{location}'"),
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE glue_like.silver.orders AS SELECT * FROM src",
    )
    .await
    .unwrap();
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT * FROM glue_like.silver.orders").await,
        3
    );
    assert!(count_parquet_files(std::path::Path::new(&location)) > 0);
}

/// WG-5 C-7: an unsupported trailing clause is a LOUD error naming the supported forms.
#[tokio::test]
async fn sql_create_namespace_unsupported_clause_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE ice.analytics AUTHORIZATION admin",
    )
    .await
    .expect_err("an unsupported CREATE NAMESPACE clause must fail loud");
    assert!(
        error
            .to_string()
            .contains("unsupported CREATE NAMESPACE clause"),
        "the error must name the unsupported clause + the supported forms, got: {error}"
    );
    assert!(
        !catalogs["ice"]
            .namespace_exists(&NamespaceIdent::new("analytics".to_string()))
            .await
            .unwrap(),
        "a fail-loud CREATE NAMESPACE must not create the namespace"
    );
}

/// F-WG5-1: CREATE NAMESPACE COMMENT round-trips into the namespace comment property.
#[tokio::test]
async fn sql_create_namespace_comment_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, _warehouse) = setup_strict_catalog(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE glue_like.documented COMMENT 'gold layer tables'",
    )
    .await
    .unwrap();

    assert_eq!(
        namespace_props(&catalogs, "documented")
            .await
            .get("comment")
            .map(String::as_str),
        Some("gold layer tables"),
        "the COMMENT clause must round-trip into the namespace `comment` property"
    );
}

/// F-WG5-1: a non-string property value parses and stores as its string form.
#[tokio::test]
async fn sql_create_namespace_number_property_value_round_trips() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, _warehouse) = setup_strict_catalog(&wh).await;

    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE glue_like.retained WITH DBPROPERTIES ('retention_days' = 7)",
    )
    .await
    .unwrap();

    assert_eq!(
        namespace_props(&catalogs, "retained")
            .await
            .get("retention_days")
            .map(String::as_str),
        Some("7"),
        "an unquoted numeric property value must store as its string form"
    );
}

/// F-WG5-1: a malformed property value fails loud naming the parse expectation.
#[tokio::test]
async fn sql_create_namespace_bad_property_value_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE ice.broken WITH DBPROPERTIES ('k' = *)",
    )
    .await
    .expect_err("a malformed property value must fail loud");
    assert!(
        error
            .to_string()
            .contains("expected a property name or value"),
        "the error must name the parse expectation, got: {error}"
    );
    assert!(
        !catalogs["ice"]
            .namespace_exists(&NamespaceIdent::new("broken".to_string()))
            .await
            .unwrap(),
        "a fail-loud CREATE NAMESPACE must not create the namespace"
    );
}

#[tokio::test]
async fn drop_namespace_nonempty_refuses_on_every_spelling() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.guarded")
        .await
        .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.guarded.t (id INT) USING iceberg",
    )
    .await
    .unwrap();
    for statement in [
        "DROP NAMESPACE ice.guarded",
        "DROP NAMESPACE ice.guarded CASCADE",
        "DROP NAMESPACE ice.guarded RESTRICT",
        "DROP NAMESPACE IF EXISTS ice.guarded",
        "DROP NAMESPACE IF EXISTS ice.guarded CASCADE",
        "DROP DATABASE ice.guarded",
        "DROP SCHEMA ice.guarded CASCADE",
    ] {
        let error = execute(&ctx, &catalogs, statement)
            .await
            .expect_err("a non-empty namespace drop must refuse");
        let message = error.to_string();
        assert!(
            message.contains("Namespace guarded is not empty."),
            "the refusal must name the namespace like Spark, got: {message}"
        );
        assert!(
            message.contains("Contains 1 table(s)."),
            "the refusal must name the table count like Spark, got: {message}"
        );
    }
    assert!(
        catalogs["ice"]
            .namespace_exists(&NamespaceIdent::new("guarded".to_string()))
            .await
            .unwrap(),
        "a refused drop must leave the namespace behind"
    );
    assert!(
        catalogs["ice"]
            .table_exists(&TableIdent::new(
                NamespaceIdent::new("guarded".to_string()),
                "t".to_string(),
            ))
            .await
            .unwrap(),
        "a refused drop must leave the table readable"
    );
}

#[tokio::test]
async fn drop_namespace_empty_drops_with_and_without_cascade() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    for (namespace, statement) in [
        ("bare", "DROP NAMESPACE ice.bare"),
        ("cascaded", "DROP NAMESPACE ice.cascaded CASCADE"),
    ] {
        execute(
            &ctx,
            &catalogs,
            &format!("CREATE NAMESPACE ice.{namespace}"),
        )
        .await
        .unwrap();
        execute(&ctx, &catalogs, statement).await.unwrap();
        assert!(
            !catalogs["ice"]
                .namespace_exists(&NamespaceIdent::new(namespace.to_string()))
                .await
                .unwrap(),
            "an empty namespace drop must remove the namespace"
        );
    }
}

#[tokio::test]
async fn drop_namespace_after_table_drop_drops() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.emptied")
        .await
        .unwrap();
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.emptied.t (id INT) USING iceberg",
    )
    .await
    .unwrap();
    execute(&ctx, &catalogs, "DROP TABLE ice.emptied.t")
        .await
        .unwrap();
    execute(&ctx, &catalogs, "DROP NAMESPACE ice.emptied")
        .await
        .unwrap();
    assert!(
        !catalogs["ice"]
            .namespace_exists(&NamespaceIdent::new("emptied".to_string()))
            .await
            .unwrap(),
        "a namespace emptied by an explicit table drop must drop"
    );
}

#[tokio::test]
async fn drop_namespace_missing_is_schema_not_found() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    let error = execute(&ctx, &catalogs, "DROP NAMESPACE ice.no_such_ns")
        .await
        .expect_err("a missing namespace drop must fail loud");
    let message = error.to_string();
    assert!(
        message.contains("[SCHEMA_NOT_FOUND]"),
        "a missing namespace must answer Spark's error class, got: {message}"
    );
    assert!(
        message.contains("`ice`.`no_such_ns`"),
        "the error must name the catalog and the namespace, got: {message}"
    );
}
