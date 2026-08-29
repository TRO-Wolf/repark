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

/// Group Z fixture: a namespace carrying every row-bearing key (`comment` / `location` /
/// `owner`) plus three user properties supplied in NON-sorted insertion order, so the
/// `Properties` rendering has to sort them itself. The `LOCATION` clause also makes the U2
/// dual-write mirror `location_uri` — which must NOT leak into `Properties`.
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

/// Z1: `DESCRIBE NAMESPACE` returns Spark's exact column shape — names, Utf8 types,
/// nullability (`info_name` NOT NULL, `info_value` nullable) and the field-level `comment`
/// metadata — plus the v2 row set, in the oracle's order, from the real namespace properties.
///
/// Live oracle rows for a fully-populated v2 namespace: Catalog Name / Namespace Name /
/// Comment / Location / Owner, and NO `Properties` row without `EXTENDED`.
///
/// MUTATION: rename `info_name` to `col_name` (or flip either nullability, or drop the field
/// metadata) in `describe_namespace_batch` → RED.
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

/// Z1 (the v2 semantics that differ from v1): a row whose backing property is ABSENT is
/// OMITTED, not emitted as an empty string. Live oracle: a v2 namespace with empty metadata
/// returns only Catalog Name + Namespace Name. This is also how the `Owner` divergence stays
/// honest — `RePark` never writes an `owner`, so the row simply does not appear.
///
/// MUTATION: emit `Comment`/`Location`/`Owner` unconditionally with `unwrap_or_default()` → RED.
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

/// Z2: `EXTENDED` appends the `Properties` row in Spark's rendering — the non-reserved keys
/// sorted by key (byte order: `Amid` before `k1`), each `(key,value)`, joined `", "`, wrapped
/// in one more paren pair. Non-EXTENDED omits the row entirely.
///
/// The `location_uri` the `LOCATION` clause mirrored (U2) is filtered — the disclosed Group Z
/// divergence from a naive "everything not Spark-reserved" filter.
///
/// MUTATION: drop the `if describe.extended` branch in `describe_namespace_batch` → RED (both
/// halves: the row vanishes from EXTENDED, or appears in the plain form).
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

/// Z2: with no non-reserved properties, `EXTENDED` still emits the row and its value is the
/// EMPTY STRING — not `()`, not absent (live oracle, v2 bare namespace).
///
/// MUTATION: return `"()"` instead of `String::new()` from `render_namespace_properties` → RED.
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

/// Z2: values are rendered RAW — Spark neither quotes nor escapes them. Live oracle for
/// `{"a b": "c,d", "z": "(paren)", "empty": ""}` → `((a b,c,d), (empty,), (z,(paren)))`.
///
/// MUTATION: quote either side (`('{key}','{value}')`) → RED.
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

/// Z2 (security): the redaction TRUTH TABLE, reproduced row for row from a live pyspark 4.0.0
/// v2-catalog run (2026-07-25). Spark's path is `DescribeNamespaceExec` → `SQLConf.redactOptions`
/// → `Utils.redact`, which matches the pattern against the **key OR the value** and replaces the
/// value, folding TWO defaults: `(?i)secret|password|token|access[.]?key` and `(?i)url`.
///
/// The fixture covers every discriminating case at once:
/// - key hits on pattern 1: `password`, `SeCrEt` (case-insensitive), `my_token_2` (substring),
///   `accesskey`, `access.key`;
/// - key hits on pattern 2: `jdbc_url`, `urlish`, `valueurl`;
/// - **VALUE** hits (the class a key-only predicate silently misses): `innocent` = "my password
///   is hunter2", `bare` = `"http://x/URL"` (also proving `(?i)` applies to values);
/// - SHOWN by both engines: `plain`, and the `access_key` / `ACCESS-KEY` / `dashaccess-key`
///   spellings Spark's `[.]?` separator does not cover (divergence 5 — an inherited Spark gap
///   `RePark` matches rather than over-redacting).
///
/// MUTATIONS: revert the predicate to key-only → RED on `innocent`/`bare`; drop the `url`
/// pattern → RED on `jdbc_url`/`urlish`/`valueurl`/`bare`; widen `access.key` back to
/// `access_key` → RED on `access_key`.
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
    // Negative-assert every plaintext secret the redaction is there to stop. Matched as the
    // rendered `(key,value)` pair, because the bare tokens overlap (`p1` is a substring of the
    // legitimately-shown `p10`) and a substring test would fail for the wrong reason.
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

/// Group Z divergence 3 (S3-3): the `Namespace Name` row goes through Spark's
/// `NamespaceHelper.quoted` — bare only for `[a-zA-Z0-9_]+` that is not all digits, else
/// backtick-wrapped with interior backticks doubled. Live-oracle-pinned, all six shapes.
///
/// MUTATION: emit `describe.namespace` raw → RED on every quoted case.
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

/// Group Z divergence 7 (S3-2): a LONE trailing `EXTENDED` is the namespace NAME, not the flag —
/// live oracle: `DESCRIBE NAMESPACE EXTENDED` raises `SCHEMA_NOT_FOUND` for a schema called
/// `EXTENDED`. `RePark` binds it the same way, so the statement stays an `AnalysisException`
/// (`DataFusionError::Plan`) instead of leaking a parse error; the message differs because
/// `RePark` needs a two-part name (divergence 2).
///
/// MUTATION: drop the `parser.prev_token()` rewind → the flag is eaten, no name parses, the
/// statement falls through to DataFusion and the class changes → RED.
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

/// Z3: `DESCRIBE DATABASE` / `DESCRIBE SCHEMA` / the `DESC` abbreviation are exact synonyms of
/// `DESCRIBE NAMESPACE`, with and without `EXTENDED` (live-oracle verified: all six spellings
/// returned byte-identical row sets).
///
/// MUTATION: drop the `Keyword::DATABASE` (or `SCHEMA`, or `DESC`) arm in
/// `try_parse_describe_namespace` → RED.
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

/// Z4: describing a namespace that does not exist raises the oracle's exception class. Live
/// pyspark 4.0.0 raises `AnalysisException` (condition `SCHEMA_NOT_FOUND`, SQLSTATE 42704);
/// `RePark`'s taxonomy maps `DataFusionError::Plan` → `Error::Analysis` →
/// `ErrorClass::Analysis` → `repark.errors.AnalysisException`. This test pins the VARIANT (the
/// taxonomy input); the Python-side class identity is pinned in
/// `python/repark/tests/test_describe_namespace.py`.
///
/// MUTATION: return `DataFusionError::NotImplemented` (or `External`) instead of `Plan` → the
/// variant assertion REDs, and with it the `AnalysisException` class the facade raises.
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

/// Z4 neighbour: an unregistered catalog fails loud on the catalog, not with a misleading
/// `SCHEMA_NOT_FOUND` (the catalog lookup happens first).
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

/// Group Z disclosed divergence #2: Spark resolves a single-part `DESCRIBE NAMESPACE ns`
/// against the current catalog and supports nested `cat.a.b` namespaces. `RePark`'s namespace
/// surface is two-part `catalog.namespace` everywhere (CREATE / DROP alike), so both forms fail
/// LOUD naming the expected shape rather than guessing a catalog or silently truncating.
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

/// Z6 regression: the namespace intercept must not shadow `DESCRIBE <table>`, and a table
/// literally named `namespace` / `database` / `schema` must still be describable.
///
/// The live oracle pins the disambiguation: `DESCRIBE namespace` (no name after the word)
/// describes the TABLE `namespace`; `DESCRIBE namespace.tbl` describes table `tbl` in database
/// `namespace`. `try_parse_describe_namespace` reproduces this by falling through (returning
/// `None`) whenever the keyword is not followed by a complete, statement-ending object name.
///
/// MUTATION: make `try_parse_describe_namespace` return `Some(Err(..))` instead of `None` on a
/// missing/partial object name → RED (the table describes start erroring).
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
/// The rendered rows this produces are the exact strings pyspark 4.0.0 showed:
/// `[zeta, alpha, beta, Mixed_Case9, `my ns`, `123`, `dash-name`, `weird.name`]`.
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

/// AB1: `SHOW NAMESPACES IN cat` returns the live oracle's exact column shape — ONE field
/// named `namespace`, `Utf8`, **non-nullable**, with NO field metadata — and the rows come from
/// the catalog's real `list_namespaces`, not a fixture.
///
/// Oracle schema JSON, verbatim:
/// `{"fields":[{"metadata":{},"name":"namespace","nullable":false,"type":"string"}],
/// "type":"struct"}`. Note it differs from `DESCRIBE NAMESPACE`'s frame, whose two fields DO
/// carry `comment` metadata — the two commands were captured separately, not assumed alike.
///
/// MUTATION: rename the field to `namespace_name`, flip `nullable` to `true`, or attach field
/// metadata in `show_namespaces_batch` → RED.
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

/// AB2: `SHOW SCHEMAS` and `SHOW DATABASES` are byte-identical synonyms of `SHOW NAMESPACES`,
/// and `FROM` is identical to `IN` — all four spellings oracle-confirmed to return the same
/// schema and the same rows. Both synonyms currently parse to sqlparser statements DataFusion
/// refuses outright ("Unsupported SQL statement: SHOW SCHEMAS"), so this is also the pin that
/// they are routed HERE.
///
/// MUTATION: drop the `Keyword::SCHEMAS` (or `DATABASES`, or `FROM`) arm from
/// `try_parse_show_namespaces` / `parse_show_namespaces_tail` → RED.
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

/// AB1/AB3: the row RENDERING and the row ORDER, on the live oracle's own fixture.
///
/// Spark maps each namespace through `NamespaceHelper.quoted` — every part `quoteIfNeeded`,
/// joined with `.` — and emits matches in the CATALOG's order with no sort of its own. The
/// oracle catalog returned `zeta, alpha, beta, Mixed_Case9, my ns, 123, dash-name, weird.name`
/// and pyspark printed exactly:
/// `zeta`, `alpha`, `beta`, `Mixed_Case9`, `` `my ns` ``, `` `123` ``, `` `dash-name` ``,
/// `` `weird.name` `` — unsorted, and quoted per part.
///
/// MUTATION: sort the rows in `show_namespace_rows` → RED (the order flips to
/// `` `123` ``-first); emit the raw namespace instead of `quoted_namespace` → RED (the four
/// backticked rows lose their quotes).
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
    // A nested namespace renders its FULL path from the root, part by part (oracle:
    // `SHOW NAMESPACES IN abcat.alpha` → `alpha.child1`, and `IN abcat.alpha.child1` →
    // `alpha.child1.grand`). RePark cannot REACH nested namespaces (divergence 2), but the
    // renderer is the same one and is pinned so a future nested surface inherits it.
    assert_eq!(
        quoted_namespace(
            &NamespaceIdent::from_vec(vec!["alpha".to_string(), "child 1".to_string(),]).unwrap()
        ),
        "alpha.`child 1`"
    );
}

/// AB3: the `LIKE` truth table, reproduced from the live oracle row for row.
///
/// Spark's `SHOW … LIKE` is NOT SQL `LIKE`: it is `StringUtils.filterPattern`, which trims the
/// WHOLE pattern once, splits it on a literal `|`, replaces `*` with `.*` in each alternative,
/// and FULL-matches the result as a case-insensitive Java regex against the RENDERED (quoted)
/// row. Each `(pattern, expected)` pair below was executed against pyspark 4.0.0 on the
/// fixture in [`oracle_namespaces`] — see `execute_show_namespaces`'s doc block for the
/// capture. The discriminating rows, one per rule:
///
/// - `lph` vs `*lph*` — FULL match, not substring;
/// - `ALPHA` / `AlPhA` — case-insensitive;
/// - `a?pha` — `?` is a regex QUANTIFIER, not a glob wildcard;
/// - `al%` / `bet_` — SQL-`LIKE` wildcards are literals here;
/// - `.*` — `.` is a live regex metacharacter (so this shows EVERYTHING);
/// - `dash-name` vs `` `dash-name` `` — the pattern sees the QUOTED string;
/// - `weird.name` — near-miss the engine must NOT show (the row has backticks);
/// - `[` and `alpha|[` — an invalid alternative is silently dropped, not raised;
/// - `  alpha  ` — the pattern is trimmed, but `alpha| beta` proves the trim is on the WHOLE
///   pattern, not per alternative;
/// - `alpha|zeta` — alternation does NOT reorder (catalog order wins);
/// - `al*|alpha` — a namespace matching two alternatives appears ONCE.
///
/// MUTATIONS: use `is_match` without the `\A`/`\z` anchors → `lph` starts matching, RED;
/// drop `.case_insensitive(true)` → `ALPHA`/`AlPhA`/`mixed_case9` RED; replace the regex with a
/// hand-rolled glob (`?` → any char) → `a?pha` RED; `unwrap()`/`expect()` the compiled regex instead of
/// `is_ok_and` → `[` PANICS instead of matching nothing, RED; trim each alternative instead of
/// the whole pattern → `alpha| beta` RED; filter the RAW name instead of the rendered one →
/// `dash-name` / `` `dash-name` `` RED.
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
        // C-AB-S2: shifted-but-balanced parens are a Java `PatternSyntaxException` → the
        // alternative is DROPPED (empty result). A `\A(?:…)\z` wrapper would rebalance them
        // into a VALID regex that matches — the wrapper-artifact class the Critic's
        // 64-pattern oracle diff caught. Mutation: restore the `(?:…)` wrapper → both RED.
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

/// AB3 at the USER entry point: the `LIKE` keyword is optional (the oracle accepts a bare
/// pattern literal, `SHOW NAMESPACES IN cat 'al*'`), and the pattern really reaches the filter
/// through SQL — the truth table above tests the function, this tests the statement.
///
/// MUTATION: ignore `show.pattern` in `execute_show_namespaces` → RED (every form returns both
/// namespaces).
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
///
/// Live oracle: `SHOW NAMESPACES IN nosuchcatalog` raises `AnalysisException` /
/// `SCHEMA_NOT_FOUND` / SQLSTATE 42704 for `` `spark_catalog`.`nosuchcatalog` `` — Spark falls
/// back to reading the unknown name as a NAMESPACE of the current catalog. `RePark` has no
/// fallback catalog, so it raises the registry's own error; the CLASS matches
/// (`DataFusionError::Plan` → `AnalysisException`, WG-3), the message does not (divergence 3).
/// The class-identity half of this pin is at the facade in
/// `python/repark/tests/test_show_namespaces.py`.
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

/// AB6: the two disclosed divergences fail LOUD naming the requirement, never guessing.
///
/// 1. **No `IN`/`FROM`.** Live oracle: Spark resolves a bare `SHOW NAMESPACES` against the
///    CURRENT catalog and — measured, not assumed — ignores the current NAMESPACE entirely
///    (after `USE abcat.alpha` it still listed the eight ROOT namespaces, not `alpha`'s two
///    children). `RePark` has no current-catalog concept, so the clause is required.
/// 2. **Nested `IN cat.ns`.** Live oracle lists the CHILDREN (`IN abcat.alpha` →
///    `alpha.child1`, `alpha.child2`). `RePark`'s namespaces are single-level, so a nested
///    listing would always be empty — an empty frame would read as "no children exist".
///
/// Both are `DataFusionError::Plan` → `AnalysisException`, matching the oracle's exception
/// family though not its message.
///
/// MUTATION: default a missing `IN` to any catalog (or truncate a two-part name to its first
/// part) in `parse_show_namespaces_tail` → RED.
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
    // A malformed tail is reported, not passed through to DataFusion's opaque `ShowVariable`
    // refusal (oracle: Spark raises ParseException / PARSE_SYNTAX_ERROR here — divergence 5).
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

/// Other SHOW forms remain DataFusion-owned, and relation names do not become namespace targets.
///
/// MUTATION: match on `SHOW` alone (dropping the `NAMESPACES|SCHEMAS|DATABASES` check) in
/// `try_parse_show_namespaces` → RED (the other SHOW forms start reporting namespace errors).
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
