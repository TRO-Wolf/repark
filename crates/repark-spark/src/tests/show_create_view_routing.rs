use super::super::*;
use super::common::*;
use super::describe_view_routing::{FaultCatalog, ViewFaults, register_catalog};
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::ErrorKind;

const SHOW_CREATE_REFUSAL: &str = "Error during planning: SHOW CREATE TABLE is not supported unless information_schema is enabled";

async fn prepared() -> (TempDir, SessionContext, CatalogRegistry) {
    let warehouse = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT, data STRING) USING iceberg",
    )
    .await;
    (warehouse, ctx, catalogs)
}

async fn view_location(catalogs: &CatalogRegistry, view: &str) -> String {
    catalogs["ice"]
        .load_view(&TableIdent::new(
            NamespaceIdent::new("sales".to_string()),
            view.to_string(),
        ))
        .await
        .unwrap()
        .metadata()
        .location()
        .to_string()
}

async fn show_create_text(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> String {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1, "{sql}");
    assert_eq!(
        batches[0].schema().as_ref(),
        &Schema::new(vec![Field::new("createtab_stmt", DataType::Utf8, false)]),
        "{sql}"
    );
    assert_eq!(batches[0].num_rows(), 1, "{sql}");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

fn v2_text(location: &str) -> String {
    format!(
        "CREATE VIEW ice.sales.v2 (\n  i COMMENT 'the id',\n  d)\nCOMMENT 'view doc'\n\
         TBLPROPERTIES (\n  'format-version' = '1',\n  'k' = 'v',\n  'location' = '{location}',\n  \
         'provider' = 'iceberg')\nAS\nSELECT id, data FROM ice.sales.t\n"
    )
}

async fn create_v2(ctx: &SessionContext, catalogs: &CatalogRegistry) {
    run(
        ctx,
        catalogs,
        "CREATE VIEW ice.sales.v2 (i COMMENT 'the id', d) COMMENT 'view doc' \
         TBLPROPERTIES ('k'='v') AS SELECT id, data FROM ice.sales.t",
    )
    .await;
}

fn fault(inner: Arc<dyn Catalog>, view_failure: ErrorKind) -> Arc<FaultCatalog> {
    Arc::new(FaultCatalog {
        inner,
        table_failure: None,
        view_failure: Some(view_failure),
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults::default(),
    })
}

#[tokio::test]
async fn show_create_view_without_docs_or_comment_matches_spark_v1() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.v1 AS SELECT id, data FROM ice.sales.t WHERE id > 0",
    )
    .await;
    let expected = format!(
        "CREATE VIEW ice.sales.v1 (\n  id,\n  data)\nTBLPROPERTIES (\n  'format-version' = '1',\n  \
         'location' = '{}',\n  'provider' = 'iceberg')\nAS\n\
         SELECT id, data FROM ice.sales.t WHERE id > 0\n",
        view_location(&catalogs, "v1").await
    );
    assert_eq!(
        show_create_text(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.v1").await,
        expected
    );
}

#[tokio::test]
async fn show_create_view_with_docs_comment_and_properties_matches_spark_v2() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    assert_eq!(
        show_create_text(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.v2").await,
        v2_text(&view_location(&catalogs, "v2").await)
    );
}

#[tokio::test]
async fn show_create_view_bare_name_follows_use() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    let expected = v2_text(&view_location(&catalogs, "v2").await);
    run(&ctx, &catalogs, "USE ice.sales").await;
    assert_eq!(
        show_create_text(&ctx, &catalogs, "SHOW CREATE TABLE v2").await,
        expected
    );
}

#[tokio::test]
async fn show_create_view_two_part_name_follows_use() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    let expected = v2_text(&view_location(&catalogs, "v2").await);
    run(&ctx, &catalogs, "USE ice").await;
    assert_eq!(
        show_create_text(&ctx, &catalogs, "SHOW CREATE TABLE sales.v2").await,
        expected
    );
}

#[tokio::test]
async fn show_create_view_keeps_the_stored_body_text_verbatim() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    run(&ctx, &catalogs, "USE ice.sales").await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.bare AS SELECT id FROM t",
    )
    .await;
    let expected = format!(
        "CREATE VIEW ice.sales.bare (\n  id)\nTBLPROPERTIES (\n  'format-version' = '1',\n  \
         'location' = '{}',\n  'provider' = 'iceberg')\nAS\nSELECT id FROM t\n",
        view_location(&catalogs, "bare").await
    );
    assert_eq!(
        show_create_text(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.bare").await,
        expected
    );
}

#[tokio::test]
async fn show_create_view_escapes_quotes_in_column_doc_and_comment() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.q (i COMMENT 'it\\'s') COMMENT 'o\\'clock' AS SELECT id FROM ice.sales.t",
    )
    .await;
    let expected = format!(
        "CREATE VIEW ice.sales.q (\n  i COMMENT 'it\\'s')\nCOMMENT 'o\\'clock'\n\
         TBLPROPERTIES (\n  'format-version' = '1',\n  'location' = '{}',\n  \
         'provider' = 'iceberg')\nAS\nSELECT id FROM ice.sales.t\n",
        view_location(&catalogs, "q").await
    );
    assert_eq!(
        show_create_text(&ctx, &catalogs, "SHOW CREATE TABLE ice.sales.q").await,
        expected
    );
}

#[tokio::test]
async fn show_create_view_as_serde_keeps_the_fallthrough_parse_error() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    let sql = "SHOW CREATE TABLE ice.sales.v2 AS SERDE";
    let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
    let DataFusionError::Diagnostic(_, inner) = &error else {
        panic!("expected Diagnostic, got {error:?}");
    };
    assert!(
        matches!(inner.as_ref(), DataFusionError::SQL(_, _)),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "SQL error: ParserError(\"Expected: end of statement, found: AS at Line: 1, Column: 32\")"
    );
}

#[tokio::test]
async fn show_create_bare_name_shadowed_by_a_session_table_falls_through() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .unwrap();
    ctx.register_batch("v2", batch).unwrap();
    run(&ctx, &catalogs, "USE ice.sales").await;
    let sql = "SHOW CREATE TABLE v2";
    let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
    assert!(matches!(&error, DataFusionError::Plan(_)), "{error:?}");
    assert_eq!(error.to_string(), SHOW_CREATE_REFUSAL);
}

#[tokio::test]
async fn show_create_view_load_misses_answer_table_or_view_not_found() {
    for kind in [ErrorKind::ViewNotFound, ErrorKind::FeatureUnsupported] {
        let (warehouse, ctx, mut catalogs) = prepared().await;
        create_v2(&ctx, &catalogs).await;
        let catalog = fault(catalogs["ice"].clone(), kind);
        register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
        let sql = "SHOW CREATE TABLE fault.sales.v2";
        let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
        assert!(matches!(&error, DataFusionError::Plan(_)), "{kind:?}");
        assert_eq!(
            error.to_string(),
            "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `fault`.`sales`.`v2` \
             cannot be found. Verify the spelling and correctness of the schema and catalog. If you \
             did not qualify the name with a schema, verify the current_schema() output, or qualify \
             the name with the correct schema and catalog. To tolerate the error on drop use DROP \
             VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01",
            "{kind:?}"
        );
    }
}

#[tokio::test]
async fn show_create_view_load_failure_keeps_iceberg_identity() {
    let (warehouse, ctx, mut catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    let catalog = fault(catalogs["ice"].clone(), ErrorKind::Unexpected);
    let view_calls = catalog.view_calls.clone();
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let sql = "SHOW CREATE TABLE fault.sales.v2";
    let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
    assert_eq!(
        error.to_string(),
        "External error: Unexpected => injected load_view failure"
    );
    let DataFusionError::External(inner) = &error else {
        panic!("expected External, got {error:?}");
    };
    let source = inner.downcast_ref::<iceberg::Error>().unwrap();
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    assert_eq!(
        source.to_string(),
        "Unexpected => injected load_view failure"
    );
    assert_eq!(view_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn show_create_view_exists_failure_keeps_iceberg_identity() {
    let (warehouse, ctx, mut catalogs) = prepared().await;
    create_v2(&ctx, &catalogs).await;
    let view_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: view_calls.clone(),
        faults: ViewFaults {
            view_exists_failure: Some(ErrorKind::Unexpected),
            ..ViewFaults::default()
        },
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let sql = "SHOW CREATE TABLE fault.sales.v2";
    let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
    assert_eq!(
        error.to_string(),
        "External error: Unexpected => injected view_exists failure"
    );
    let DataFusionError::External(inner) = &error else {
        panic!("expected External, got {error:?}");
    };
    let source = inner.downcast_ref::<iceberg::Error>().unwrap();
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    assert_eq!(view_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn show_create_view_without_a_sql_representation_is_a_plan_error() {
    let (warehouse, ctx, catalogs) = prepared().await;
    let schema = iceberg::spec::Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![Arc::new(iceberg::spec::NestedField::optional(
            1,
            "id",
            iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
        ))])
        .build()
        .unwrap();
    let namespace = NamespaceIdent::new("sales".to_string());
    let creation = iceberg::ViewCreation::builder()
        .name("norep".to_string())
        .location(
            warehouse
                .path()
                .join("sales/norep")
                .to_string_lossy()
                .into_owned(),
        )
        .representations(iceberg::spec::ViewRepresentations::new(Vec::new()))
        .schema(schema)
        .default_namespace(namespace.clone())
        .build();
    catalogs["ice"]
        .create_view(&namespace, creation)
        .await
        .unwrap();
    let sql = "SHOW CREATE TABLE ice.sales.norep";
    let error = execute(&ctx, &catalogs, sql).await.expect_err(sql);
    assert!(matches!(&error, DataFusionError::Plan(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "Error during planning: view `norep` has a current version with no SQL representation"
    );
}

#[tokio::test]
async fn show_create_view_on_an_unregistered_catalog_is_a_plan_error() {
    let (_warehouse, ctx, catalogs) = prepared().await;
    let statement = crate::show_create::ShowCreateStatement {
        catalog: "nope".to_string(),
        namespace: "sales".to_string(),
        table: "v2".to_string(),
        as_serde: false,
    };
    let error = crate::view_ddl::show_create::execute_show_create_view(&ctx, &catalogs, &statement)
        .await
        .expect_err("an unregistered catalog must refuse");
    assert!(matches!(&error, DataFusionError::Plan(_)), "{error:?}");
    assert_eq!(
        error.to_string(),
        "Error during planning: unknown catalog `nope`"
    );
}
