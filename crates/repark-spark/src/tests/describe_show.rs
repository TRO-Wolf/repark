/// Group Z helper: run a `DESCRIBE NAMESPACE …` and return its `(info_name, info_value)` rows.
use super::super::*;
use super::common::*;

async fn describe_rows(
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
    let mut rows = Vec::new();
    for batch in &batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                names.value(index).to_string(),
                values.value(index).to_string(),
            ));
        }
    }
    rows
}

/// Group Z fixture: a namespace carrying comment, location, owner, plus unsorted user properties.
async fn create_described_namespace(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    execute(
        ctx,
        catalogs,
        "CREATE NAMESPACE ice.described COMMENT 'z full comment' \
             LOCATION 's3://bucket/z/full' \
             WITH DBPROPERTIES ('owner' = 'zowner', 'k2' = 'v2', 'k1' = 'v1', 'Amid' = 'vm')",
    )
    .await
    .unwrap();
}

/// MUTATION: rename `info_name` to `col_name` (or flip either nullability, or drop the field metadata) in `describe_namespace_batch` → RED.
/// Z1: `DESCRIBE NAMESPACE` returns Spark's exact column shape.
#[tokio::test]
async fn describe_namespace_returns_spark_column_shape_and_rows() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_described_namespace(&ctx, &catalogs).await;

    let frame = execute(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.described")
        .await
        .unwrap();
    let schema = frame.schema();
    let fields: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        fields,
        vec!["info_name", "info_value"],
        "Spark's DESCRIBE NAMESPACE columns are info_name/info_value"
    );
    assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    assert!(
        !schema.field(0).is_nullable(),
        "info_name is NOT NULL in Spark's schema"
    );
    assert!(
        schema.field(1).is_nullable(),
        "info_value is nullable in Spark's schema"
    );
    assert_eq!(
        schema
            .field(0)
            .metadata()
            .get("comment")
            .map(String::as_str),
        Some("name of the namespace info")
    );
    assert_eq!(
        schema
            .field(1)
            .metadata()
            .get("comment")
            .map(String::as_str),
        Some("value of the namespace info")
    );

    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.described").await;
    assert_eq!(
        rows,
        vec![
            ("Catalog Name".to_string(), "ice".to_string()),
            ("Namespace Name".to_string(), "described".to_string()),
            ("Comment".to_string(), "z full comment".to_string()),
            ("Location".to_string(), "s3://bucket/z/full".to_string()),
            ("Owner".to_string(), "zowner".to_string()),
        ]
    );
}

/// MUTATION: emit `Comment`/`Location`/`Owner` unconditionally with `unwrap_or_default()` → RED.
/// Z1: a row whose backing property is ABSENT is OMITTED, not emitted as an empty string.
#[tokio::test]
async fn describe_namespace_omits_rows_whose_property_is_absent() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.bare")
        .await
        .unwrap();

    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.bare").await,
        vec![
            ("Catalog Name".to_string(), "ice".to_string()),
            ("Namespace Name".to_string(), "bare".to_string()),
        ]
    );
}

/// MUTATION: drop the `if describe.extended` branch in `describe_namespace_batch` → RED (both halves: the row vanishes from EXTENDED, or appears in the plain form).
/// Z2: `EXTENDED` appends the `Properties` row in Spark's rendering.
#[tokio::test]
async fn describe_namespace_extended_adds_the_properties_row() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_described_namespace(&ctx, &catalogs).await;

    let extended =
        describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.described").await;
    assert_eq!(
        extended.last(),
        Some(&(
            "Properties".to_string(),
            "((Amid,vm), (k1,v1), (k2,v2))".to_string()
        )),
        "Spark renders ((k,v), …) sorted by key, with the reserved keys filtered"
    );
    assert_eq!(extended.len(), 6);
    assert!(
        !extended
            .iter()
            .any(|(_, value)| value.contains("location_uri")),
        "the U2 location_uri mirror must not leak into Properties"
    );

    let plain = describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.described").await;
    assert!(
        !plain.iter().any(|(name, _)| name == "Properties"),
        "without EXTENDED there is no Properties row"
    );
}

/// MUTATION: return `"()"` instead of `String::new()` from `render_namespace_properties` → RED.
/// Z2: with no user properties, EXTENDED still emits Properties as the empty string.
#[tokio::test]
async fn describe_namespace_extended_empty_properties_render_as_empty_string() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE ice.onlyloc LOCATION 's3://bucket/z/loconly'",
    )
    .await
    .unwrap();

    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.onlyloc").await,
        vec![
            ("Catalog Name".to_string(), "ice".to_string()),
            ("Namespace Name".to_string(), "onlyloc".to_string()),
            ("Location".to_string(), "s3://bucket/z/loconly".to_string()),
            ("Properties".to_string(), String::new()),
        ]
    );
}

/// MUTATION: quote either side (`('{key}','{value}')`) → RED.
/// Z2: values are rendered RAW — Spark neither quotes nor escapes them.
#[tokio::test]
async fn describe_namespace_extended_renders_property_values_raw() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE ice.weird \
             WITH DBPROPERTIES ('a b' = 'c,d', 'z' = '(paren)', 'empty' = '')",
    )
    .await
    .unwrap();

    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.weird").await;
    assert_eq!(
        rows.last(),
        Some(&(
            "Properties".to_string(),
            "((a b,c,d), (empty,), (z,(paren)))".to_string()
        ))
    );
}

/// Z2: the redaction TRUTH TABLE, reproduced row for row from a live pyspark 4.0.0 v2-catalog run.
#[tokio::test]
async fn describe_namespace_extended_redaction_truth_table() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE NAMESPACE ice.creds WITH DBPROPERTIES ( \
             'password' = 'p1', 'SeCrEt' = 'p2', 'my_token_2' = 'p3', 'accesskey' = 'p4', \
             'access.key' = 'p5', 'ACCESS-KEY' = 'p6', 'plain' = 'p7', 'access_key' = 'p8', \
             'innocent' = 'my password is hunter2', 'jdbc_url' = 'jdbc://u:pw@h/db', \
             'urlish' = 'p9', 'valueurl' = 'http://x/URL', 'bare' = 'http://x/URL', \
             'dashaccess-key' = 'p10')",
    )
    .await
    .unwrap();

    let rows = describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.creds").await;
    let (_, properties) = rows.last().unwrap();
    // Verbatim from the live oracle.
    assert_eq!(
        properties,
        "((ACCESS-KEY,p6), (SeCrEt,*********(redacted)), (access.key,*********(redacted)), \
             (access_key,p8), (accesskey,*********(redacted)), (bare,*********(redacted)), \
             (dashaccess-key,p10), (innocent,*********(redacted)), (jdbc_url,*********(redacted)), \
             (my_token_2,*********(redacted)), (password,*********(redacted)), (plain,p7), \
             (urlish,*********(redacted)), (valueurl,*********(redacted)))",
        "the rendered Properties string must match live Spark byte for byte"
    );
    // Negative-assert every plaintext secret the redaction is there to stop.
    for (key, secret) in [
        ("password", "p1"),
        ("SeCrEt", "p2"),
        ("my_token_2", "p3"),
        ("accesskey", "p4"),
        ("access.key", "p5"),
        ("urlish", "p9"),
        ("innocent", "my password is hunter2"),
        ("jdbc_url", "jdbc://u:pw@h/db"),
        ("valueurl", "http://x/URL"),
        ("bare", "http://x/URL"),
    ] {
        assert!(
            !properties.contains(&format!("({key},{secret})")),
            "the secret for {key} must never reach DESCRIBE output: {properties}"
        );
    }
    // The value-bearing secrets are unique enough to also assert absent outright.
    for secret in ["hunter2", "jdbc://u:pw@h/db", "http://x/URL"] {
        assert!(
            !properties.contains(secret),
            "the secret {secret} must never reach DESCRIBE output: {properties}"
        );
    }
}

/// MUTATION: emit `describe.namespace` raw → RED on every quoted case.
/// Group Z divergence 3: the `Namespace Name` row goes through Spark's `NamespaceHelper.quoted`.
#[tokio::test]
async fn describe_namespace_name_row_is_quoted_like_spark() {
    let wh = TempDir::new().unwrap();
    let (_ctx, catalogs) = setup(&wh).await;
    // (namespace, the `Namespace Name` value live Spark renders)
    let cases = [
        ("Mixed_Case9", "Mixed_Case9"),
        ("my ns", "`my ns`"),
        ("weird.name", "`weird.name`"),
        ("dash-name", "`dash-name`"),
        ("123", "`123`"),
        ("has`tick", "`has``tick`"),
    ];
    for (namespace, _) in cases {
        catalogs["ice"]
            .create_namespace(&NamespaceIdent::new(namespace.to_string()), HashMap::new())
            .await
            .unwrap();
    }
    for (namespace, expected) in cases {
        let describe = DescribeNamespace {
            catalog: "ice".to_string(),
            namespace: namespace.to_string(),
            extended: false,
        };
        let batch = describe_namespace_batch(&describe, &HashMap::new()).unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            values.value(1),
            expected,
            "Namespace Name for {namespace:?} must match Spark's quoted() rendering"
        );
    }
}

/// MUTATION: drop the `parser.prev_token()` rewind → the flag is eaten, no name parses, the statement falls through to DataFusion and the class changes → RED.
/// Group Z divergence 7: a LONE trailing `EXTENDED` is the namespace NAME, not the flag.
#[tokio::test]
async fn describe_namespace_lone_trailing_extended_is_the_name() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    for sql in [
        "DESCRIBE NAMESPACE EXTENDED",
        "DESCRIBE DATABASE EXTENDED",
        "DESC SCHEMA EXTENDED",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a lone trailing EXTENDED names a namespace that cannot resolve");
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql} must stay in the AnalysisException class like Spark, got: {error:?}"
        );
        assert!(
            error.to_string().contains("two-part `catalog.namespace`"),
            "{sql} must name the shape it needs, got: {error}"
        );
    }
}

/// MUTATION: drop the `Keyword::DATABASE` (or `SCHEMA`, or `DESC`) arm in `try_parse_describe_namespace` → RED.
/// Z3: DESCRIBE DATABASE, SCHEMA, and DESC are synonyms of DESCRIBE NAMESPACE.
#[tokio::test]
async fn describe_database_and_schema_synonyms_are_identical() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_described_namespace(&ctx, &catalogs).await;

    let baseline = describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.described").await;
    for sql in [
        "DESCRIBE DATABASE ice.described",
        "DESCRIBE SCHEMA ice.described",
        "DESC NAMESPACE ice.described",
        "DESC DATABASE ice.described",
        "DESC SCHEMA ice.described",
    ] {
        assert_eq!(
            describe_rows(&ctx, &catalogs, sql).await,
            baseline,
            "{sql} must match DESCRIBE NAMESPACE exactly"
        );
    }

    let extended =
        describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE EXTENDED ice.described").await;
    for sql in [
        "DESCRIBE DATABASE EXTENDED ice.described",
        "DESCRIBE SCHEMA EXTENDED ice.described",
        "DESC NAMESPACE EXTENDED ice.described",
    ] {
        assert_eq!(
            describe_rows(&ctx, &catalogs, sql).await,
            extended,
            "{sql} must match DESCRIBE NAMESPACE EXTENDED exactly"
        );
    }
}

/// MUTATION: return `DataFusionError::NotImplemented` (or `External`) instead of `Plan` → the variant assertion REDs, and with it the `AnalysisException` class the facade raises.
/// Z4: describing a namespace that does not exist raises the oracle's exception class.
#[tokio::test]
async fn describe_namespace_missing_raises_schema_not_found_as_analysis() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.no_such_ns")
        .await
        .expect_err("describing a missing namespace must fail loud");
    assert!(
        matches!(error, DataFusionError::Plan(_)),
        "Plan is the variant repark-core classifies Analysis → AnalysisException, got: {error:?}"
    );
    let message = error.to_string();
    assert!(
        message.contains("[SCHEMA_NOT_FOUND]") && message.contains("`no_such_ns`"),
        "the message must carry Spark's condition and name the namespace, got: {message}"
    );
    assert!(
        execute(
            &ctx,
            &catalogs,
            "DESCRIBE NAMESPACE EXTENDED ice.no_such_ns"
        )
        .await
        .is_err(),
        "EXTENDED takes the same missing-namespace path"
    );
}

/// Z4 neighbour: an unregistered catalog fails loud on the catalog, not.
#[tokio::test]
async fn describe_namespace_unknown_catalog_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    let error = execute(&ctx, &catalogs, "DESCRIBE NAMESPACE nosuch.ns")
        .await
        .expect_err("an unregistered catalog must fail loud");
    assert!(
        error.to_string().contains("unknown catalog `nosuch`"),
        "got: {error}"
    );
}

/// Group Z disclosed divergence #2.
#[tokio::test]
async fn describe_namespace_non_two_part_name_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    for sql in [
        "DESCRIBE NAMESPACE sales",
        "DESCRIBE NAMESPACE EXTENDED ice.nested.deeper",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a non-two-part namespace name must fail loud");
        assert!(
            error.to_string().contains("two-part `catalog.namespace`"),
            "{sql} must name the expected shape, got: {error}"
        );
    }
}

/// MUTATION: make `try_parse_describe_namespace` return `Some(Err(..))` instead of `None` on a missing/partial object name → RED (the table describes start erroring).
/// Z6 regression: the namespace intercept must not shadow `DESCRIBE <table>`.
#[tokio::test]
async fn describe_table_is_not_shadowed_by_the_namespace_intercept() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    create_described_namespace(&ctx, &catalogs).await;
    // Tables whose names collide with the namespace keywords.
    register_source(&ctx, "namespace", &[(1, "a")]);
    register_source(&ctx, "database", &[(2, "b")]);
    register_source(&ctx, "schema", &[(3, "c")]);

    for sql in [
        "DESCRIBE namespace",
        "DESCRIBE database",
        "DESCRIBE schema",
        "DESC namespace",
        "DESCRIBE src",
    ] {
        let frame = execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql} must still describe the TABLE: {error}"));
        let first = frame.schema().field(0).name().clone();
        assert_ne!(
            first, "info_name",
            "{sql} must NOT be routed to the namespace describe"
        );
        let rows: usize = frame
            .collect()
            .await
            .unwrap()
            .iter()
            .map(RecordBatch::num_rows)
            .sum();
        assert_eq!(rows, 2, "{sql} describes the two-column table");
    }

    // The namespace form itself still works alongside them.
    assert_eq!(
        describe_rows(&ctx, &catalogs, "DESCRIBE NAMESPACE ice.described")
            .await
            .first()
            .map(|(name, _)| name.clone()),
        Some("Catalog Name".to_string())
    );
}

/// The live oracle's namespace fixture, in the catalog's own (deliberately unsorted) order.
fn oracle_namespaces() -> Vec<NamespaceIdent> {
    [
        "zeta",
        "alpha",
        "beta",
        "Mixed_Case9",
        "my ns",
        "123",
        "dash-name",
        "weird.name",
    ]
    .into_iter()
    .map(|name| NamespaceIdent::new(name.to_string()))
    .collect()
}

/// The `namespace` column of a `SHOW NAMESPACES` frame, in frame order.
async fn show_rows(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<String> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|value| value.unwrap().to_string())
                .collect::<Vec<String>>()
        })
        .collect()
}

/// MUTATION: rename the field to `namespace_name`, flip `nullable` to `true`, or attach field metadata in `show_namespaces_batch` → RED.
/// AB1: `SHOW NAMESPACES IN cat` returns the live oracle's exact column shape.
#[tokio::test]
async fn show_namespaces_returns_spark_column_shape_and_real_namespaces() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.marketing")
        .await
        .unwrap();

    let frame = execute(&ctx, &catalogs, "SHOW NAMESPACES IN ice")
        .await
        .unwrap();
    let schema = frame.schema();
    assert_eq!(
        schema.fields().len(),
        1,
        "the oracle frame has exactly one column"
    );
    let field = schema.field(0);
    assert_eq!(field.name(), "namespace");
    assert_eq!(field.data_type(), &DataType::Utf8);
    assert!(!field.is_nullable(), "the oracle column is NOT NULL");
    assert!(
        field.metadata().is_empty(),
        "the oracle column carries no field metadata, got: {:?}",
        field.metadata()
    );

    let mut rows = show_rows(&ctx, &catalogs, "SHOW NAMESPACES IN ice").await;
    rows.sort();
    assert_eq!(
        rows,
        vec!["marketing".to_string(), "sales".to_string()],
        "the rows are the catalog's real namespaces (`sales` from setup + `marketing`)"
    );
}

/// MUTATION: drop the `Keyword::SCHEMAS` (or `DATABASES`, or `FROM`) arm from `try_parse_show_namespaces` / `parse_show_namespaces_tail` → RED.
/// AB2: `SHOW SCHEMAS` and `SHOW DATABASES` are byte-identical synonyms of `SHOW NAMESPACES`.
#[tokio::test]
async fn show_schemas_and_databases_synonyms_are_identical() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.marketing")
        .await
        .unwrap();

    let mut expected = show_rows(&ctx, &catalogs, "SHOW NAMESPACES IN ice").await;
    expected.sort();
    assert_eq!(expected, vec!["marketing".to_string(), "sales".to_string()]);

    for sql in [
        "SHOW SCHEMAS IN ice",
        "SHOW DATABASES IN ice",
        "SHOW NAMESPACES FROM ice",
        "SHOW SCHEMAS FROM ice",
        "SHOW DATABASES FROM ice",
        "show namespaces in ice",
        "SHOW NAMESPACES IN ice;",
    ] {
        let frame = execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql} must be a SHOW NAMESPACES synonym: {error}"));
        assert_eq!(
            frame.schema().field(0).name(),
            "namespace",
            "{sql} must produce the namespace frame"
        );
        let mut rows = show_rows(&ctx, &catalogs, sql).await;
        rows.sort();
        assert_eq!(rows, expected, "{sql} must return the identical row set");
    }
}

/// MUTATION: sort the rows in `show_namespace_rows` → RED (the order flips to `` `123` ``-first); emit the raw namespace instead of `quoted_namespace` → RED (the four backticked rows lose their quotes).
/// AB1/AB3: the row RENDERING and the row ORDER, on the live oracle's own fixture.
#[test]
fn show_namespace_rows_are_quoted_like_spark_and_keep_catalog_order() {
    assert_eq!(
        show_namespace_rows(&oracle_namespaces(), None),
        vec![
            "zeta",
            "alpha",
            "beta",
            "Mixed_Case9",
            "`my ns`",
            "`123`",
            "`dash-name`",
            "`weird.name`",
        ],
        "the live oracle's rows, in the live oracle's (catalog) order"
    );
    // A nested namespace renders its FULL path from the root, part by part.
    assert_eq!(
        quoted_namespace(
            &NamespaceIdent::from_vec(vec!["alpha".to_string(), "child 1".to_string(),]).unwrap()
        ),
        "alpha.`child 1`"
    );
}

/// AB3: the `LIKE` truth table, reproduced from the live oracle row for row.
#[test]
fn show_namespaces_like_truth_table() {
    let namespaces = oracle_namespaces();
    let all = vec![
        "zeta",
        "alpha",
        "beta",
        "Mixed_Case9",
        "`my ns`",
        "`123`",
        "`dash-name`",
        "`weird.name`",
    ];
    let cases: Vec<(&str, Vec<&str>)> = vec![
        ("alpha", vec!["alpha"]),
        ("ALPHA", vec!["alpha"]),
        ("AlPhA", vec!["alpha"]),
        ("lph", vec![]),
        ("*lph*", vec!["alpha"]),
        ("al*", vec!["alpha"]),
        ("*ta", vec!["zeta", "beta"]),
        ("*et*", vec!["zeta", "beta"]),
        ("a?pha", vec![]),
        ("al%", vec![]),
        ("bet_", vec![]),
        ("dash-name", vec![]),
        ("`dash-name`", vec!["`dash-name`"]),
        ("*dash-name*", vec!["`dash-name`"]),
        ("weird.name", vec![]),
        ("weird?name", vec![]),
        ("my ns", vec![]),
        ("*my ns*", vec!["`my ns`"]),
        ("123", vec![]),
        ("*123*", vec!["`123`"]),
        ("Mixed_Case9", vec!["Mixed_Case9"]),
        ("mixed_case9", vec!["Mixed_Case9"]),
        (".*", all.clone()),
        ("*", all.clone()),
        ("", vec![]),
        ("zzz", vec![]),
        ("  alpha  ", vec!["alpha"]),
        ("alpha| beta", vec!["alpha"]),
        ("alpha|zeta", vec!["zeta", "alpha"]),
        ("alpha|beta", vec!["alpha", "beta"]),
        ("al*|alpha", vec!["alpha"]),
        ("[", vec![]),
        // C-AB-S2: shifted-but-balanced parens drop the LIKE alternative as Java does.
        ("alpha)(", vec![]),
        ("a)(b", vec![]),
        ("alpha|[", vec!["alpha"]),
    ];
    for (pattern, expected) in cases {
        assert_eq!(
            show_namespace_rows(&namespaces, Some(pattern)),
            expected,
            "live pyspark 4.0.0 showed {expected:?} for LIKE '{pattern}'"
        );
    }
}

/// MUTATION: ignore `show.pattern` in `execute_show_namespaces` → RED (every form returns both namespaces).
/// AB3 at the USER entry point.
#[tokio::test]
async fn show_namespaces_like_filters_through_sql() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(&ctx, &catalogs, "CREATE NAMESPACE ice.marketing")
        .await
        .unwrap();

    for sql in [
        "SHOW NAMESPACES IN ice LIKE 'sal*'",
        "SHOW NAMESPACES IN ice 'sal*'",
        "SHOW SCHEMAS FROM ice LIKE 'SALES'",
        "SHOW DATABASES IN ice LIKE 'sales|nope'",
    ] {
        assert_eq!(
            show_rows(&ctx, &catalogs, sql).await,
            vec!["sales".to_string()],
            "{sql} must filter to `sales`"
        );
    }
    assert!(
        show_rows(&ctx, &catalogs, "SHOW NAMESPACES IN ice LIKE 'nope*'")
            .await
            .is_empty(),
        "a non-matching pattern returns ZERO rows, not an error (oracle: empty frame)"
    );
    // A `LIKE` with no pattern is a loud parse-class error, not a silent show-everything.
    let error = execute(&ctx, &catalogs, "SHOW NAMESPACES IN ice LIKE")
        .await
        .expect_err("LIKE without a pattern must fail loud");
    assert!(
        error.to_string().contains("needs a quoted pattern"),
        "got: {error}"
    );
}

/// AB4: an unregistered catalog fails loud with the oracle's exception CLASS.
#[tokio::test]
async fn show_namespaces_unknown_catalog_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    for sql in [
        "SHOW NAMESPACES IN nosuch",
        "SHOW SCHEMAS IN nosuch",
        "SHOW DATABASES FROM nosuch LIKE '*'",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("an unregistered catalog must fail loud");
        assert!(
            matches!(error, DataFusionError::Plan(_)),
            "{sql} must be plan-class (→ AnalysisException), got: {error:?}"
        );
        assert!(
            error.to_string().contains("unknown catalog `nosuch`"),
            "{sql} got: {error}"
        );
    }
}

/// MUTATION: default a missing `IN` to any catalog (or truncate a two-part name to its first part) in `parse_show_namespaces_tail` → RED.
/// AB6: the two disclosed divergences fail LOUD naming the requirement, never guessing.
#[tokio::test]
async fn show_namespaces_without_a_catalog_or_with_a_nested_name_fails_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;

    for sql in ["SHOW NAMESPACES", "SHOW SCHEMAS", "SHOW DATABASES LIKE '*'"] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("RePark has no current catalog — this must fail loud");
        assert!(
            error.to_string().contains("requires an explicit catalog"),
            "{sql} must name the requirement, got: {error}"
        );
    }
    for sql in [
        "SHOW NAMESPACES IN ice.sales",
        "SHOW NAMESPACES IN ice.a.b LIKE '*'",
    ] {
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("a nested namespace listing must fail loud");
        assert!(
            error.to_string().contains("one-part `IN <catalog>`"),
            "{sql} must name the expected shape, got: {error}"
        );
    }
    // A malformed SHOW tail is reported, not passed through to DataFusion's ShowVariable refusal.
    let error = execute(&ctx, &catalogs, "SHOW NAMESPACES IN ice GARBAGE")
        .await
        .expect_err("a malformed tail must fail loud");
    assert!(
        error
            .to_string()
            .contains("could not parse `SHOW NAMESPACES`"),
        "got: {error}"
    );
}

/// MUTATION: match on `SHOW` alone (dropping the `NAMESPACES|SCHEMAS|DATABASES` check) in `try_parse_show_namespaces` → RED (the other SHOW forms start reporting namespace errors).
/// Other SHOW forms remain DataFusion-owned, and relation names do not become namespace targets.
#[tokio::test]
async fn show_namespaces_intercept_shadows_no_other_statement() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    register_source(&ctx, "namespaces", &[(1, "a")]);
    register_source(&ctx, "schemas", &[(2, "b")]);
    register_source(&ctx, "databases", &[(3, "c")]);

    for sql in [
        "SHOW TABLES",
        "SHOW TABLES IN ice.sales",
        "SHOW COLUMNS FROM src",
        "SHOW VIEWS",
        "SHOW ALL",
    ] {
        // Unsupported SHOW form must fail without a namespace error.
        let error = execute(&ctx, &catalogs, sql)
            .await
            .expect_err("no other SHOW form works on this base commit");
        let message = error.to_string();
        assert!(
            !message.contains("SHOW NAMESPACES") && !message.contains("unknown catalog"),
            "{sql} must keep DataFusion's own refusal, got: {message}"
        );
    }

    // Relations whose names collide with the keywords are still readable and describable.
    for sql in [
        "SELECT * FROM namespaces",
        "SELECT * FROM schemas",
        "SELECT * FROM databases",
        "DESCRIBE namespaces",
    ] {
        execute(&ctx, &catalogs, sql)
            .await
            .unwrap_or_else(|error| panic!("{sql} must still work: {error}"));
    }
}
