/// `CREATE NAMESPACE` / `DROP NAMESPACE` against the catalog, with `IF [NOT] EXISTS` idempotency.
use super::super::*;
use super::common::*;

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

/// WG-5 C-1: SQL `CREATE NAMESPACE … LOCATION '/x'` on a **strict** `RequireExplicitLocation`
/// catalog lets a subsequent CTAS succeed with its data landing under `/x` — the ADV-2 residual
/// closed (previously only the programmatic `create_namespace(..., location=…)` could set it).
/// Value-checked on both the read-back rows and the physical `.parquet` placement.
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

/// U2-P6 (the SQL writer's dual-write): SQL `CREATE NAMESPACE … LOCATION '/x'` stores BOTH
/// `location` AND `location_uri` = `/x` in the namespace metadata — so the canonical Glue
/// `locationUri` field is set whichever key the catalog implementation maps (fork:
/// `location_uri`; Java: `location`), closing the audit's "`RePark` namespaces never set the
/// canonical field other engines read" hole. The CTAS then proves the dual-keyed map resolves.
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

/// U2-P7 (non-clobbering + unidirectional, the D-U2-4 write contract): an explicitly-set
/// `location_uri` is NEVER overwritten by the mirror (`LOCATION 'a' WITH DBPROPERTIES
/// ('location_uri' = 'b')` keeps b — and the CTAS still lands under a, the read precedence);
/// and a `location_uri`-only DBPROPERTIES create stays single-key (no synthesized
/// `location`). Risk pinned: a clobbering mirror destroys explicit user input; a
/// bidirectional mirror fabricates a key the user never set.
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

/// WG-5 C-2: `WITH DBPROPERTIES ('location' = '/x', …)` round-trips into the namespace metadata,
/// and the `location` key is load-bearing (it drives the CTAS placement). Strict catalog.
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

/// WG-5 C-2 (PROPERTIES synonym): Spark accepts `WITH PROPERTIES (…)` as well as
/// `WITH DBPROPERTIES (…)`; the `location` round-trips and drives the CTAS placement.
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

/// WG-5 C-3: `IF NOT EXISTS` is idempotent — a second create on an existing namespace is a no-op
/// that does NOT error and does NOT overwrite the existing `location` (so a later CTAS still
/// lands under the ORIGINAL location, not the second call's).
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
    // A second create with IF NOT EXISTS (pointing at a DIFFERENT location) is a no-op.
    execute(
        &ctx,
        &catalogs,
        &format!("CREATE NAMESPACE IF NOT EXISTS glue_like.silver LOCATION '{warehouse}/other'"),
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

/// WG-5 C-7: `CREATE DATABASE` is a synonym for `CREATE NAMESPACE` — it now routes through the
/// same handler (previously `Statement::CreateDatabase` fell to passthrough and never created an
/// Iceberg namespace).
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

/// WG-5 C-7: an unsupported trailing clause (here the SQL-standard `AUTHORIZATION`, which
/// sqlparser's `CREATE SCHEMA` models but Spark's namespace surface does not) is a LOUD error
/// naming the supported forms — never a silent drop — and leaves no namespace behind.
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

/// F-WG5-1 (W51-1): `CREATE NAMESPACE … COMMENT '…'` round-trips the comment into the namespace
/// `comment` property (Spark's namespace comment clause). Mutation: drop the `COMMENT` arm in
/// `parse_create_namespace_body` → the comment is never stored → RED.
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

/// F-WG5-1 (W51-2): a non-string (bare number) property value parses and stores as its string
/// form — Spark accepts unquoted numeric property values. Mutation: drop the `Token::Number` arm
/// in `parse_namespace_property_string` → the number no longer parses → RED (parse error).
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

/// F-WG5-1 (W51-3): a malformed property value (a token that is neither a word, a quoted
/// string, nor a number) fails loud naming the parse expectation — never a silent drop — and no
/// namespace is created. Mutation: relax the `other =>` arm in `parse_namespace_property_string`
/// → RED. (Distinct error path from the trailing-clause `unsupported CREATE NAMESPACE clause`.)
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
