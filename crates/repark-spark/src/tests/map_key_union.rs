use super::super::*;
use super::accept_any_refusals::door;
use super::common::*;

const ACCEPT_ANY: &str = "TBLPROPERTIES ('write.spark.accept-any-schema'='true')";

const DEEP_KEY: &str = "mk MAP<STRUCT<s: STRUCT<x: INT>, a: INT>, INT>";

const ALTER_DEEP: &str = "Unsupported table change: Cannot alter map keys: \
     map<struct<#: s: optional struct<#: x: optional int>, #: a: optional int>, int>";

const ADD_DEEP: &str = "Unsupported table change: Cannot add fields to map keys: \
     map<struct<#: s: optional struct<#: x: optional int>, #: a: optional int>, int>";

const BY_NAME: &str = "INSERT INTO ice.sales.t BY NAME SELECT * FROM ice.sales.src";

const POSITIONAL: &str = "INSERT INTO ice.sales.t SELECT * FROM ice.sales.src";

const EVOLVING_MERGE: &str = "MERGE WITH SCHEMA EVOLUTION INTO ice.sales.t t USING ice.sales.src s \
     ON t.id = s.id WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *";

fn without_ids(message: &str) -> String {
    regex::Regex::new(r"\b\d+: ")
        .unwrap()
        .replace_all(message, "#: ")
        .into_owned()
}

fn target_files(wh: &TempDir) -> usize {
    let mut count = 0;
    let mut stack = vec![wh.path().to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else if path.components().any(|part| part.as_os_str() == "t") {
                count += 1;
            }
        }
    }
    count
}

async fn target_state(catalogs: &CatalogRegistry) -> (String, String) {
    let table = load_sales_table(catalogs, "t").await;
    (
        table.metadata_location().unwrap_or_default().to_string(),
        format!("{:?}", table.metadata().current_schema().as_struct()),
    )
}

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> std::result::Result<(), String> {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn tables(
    target: &str,
    source: &str,
    accept_any: bool,
) -> (TempDir, SessionContext, CatalogRegistry) {
    let wh = TempDir::new().unwrap();
    let properties = if accept_any { ACCEPT_ANY } else { "" };
    let create = format!("CREATE TABLE ice.sales.t (id INT, {target}) USING iceberg {properties}");
    let (ctx, catalogs) = door(&wh, &create, true).await;
    run(
        &ctx,
        &catalogs,
        &format!("CREATE TABLE ice.sales.src (id INT, {source}) USING iceberg"),
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.src (id) VALUES (1)").await;
    (wh, ctx, catalogs)
}

async fn refuses_unchanged(
    target: &str,
    source: &str,
    accept_any: bool,
    sql: &str,
    expected: &str,
) {
    let (wh, ctx, catalogs) = tables(target, source, accept_any).await;
    let before = (target_state(&catalogs).await, target_files(&wh));
    let error = outcome(&ctx, &catalogs, sql).await.expect_err(sql);
    assert!(
        without_ids(&error).contains(expected),
        "{source} / {sql}: `{error}` lacks `{expected}`"
    );
    assert_eq!(
        (target_state(&catalogs).await, target_files(&wh)),
        before,
        "{source} / {sql}"
    );
}

async fn evolves(target: &str, source: &str, accept_any: bool, sql: &str) {
    let (_wh, ctx, catalogs) = tables(target, source, accept_any).await;
    let before = target_state(&catalogs).await;
    outcome(&ctx, &catalogs, sql).await.expect(sql);
    let after = target_state(&catalogs).await;
    assert_ne!(after.1, before.1, "{source} / {sql}");
    assert!(after.1.contains("\"z\""), "{source} / {sql}: {}", after.1);
}

#[tokio::test]
async fn a_union_that_adds_or_widens_below_a_map_key_refuses_on_every_evolving_write() {
    for source in [
        "mk MAP<STRUCT<s: STRUCT<x: INT, z: INT>, a: INT>, INT>",
        "mk MAP<STRUCT<s: STRUCT<x: BIGINT>, a: INT>, INT>",
        "mk MAP<STRUCT<s: STRUCT<x: BIGINT, z: INT>, a: BIGINT>, INT>",
    ] {
        refuses_unchanged(DEEP_KEY, source, true, BY_NAME, ALTER_DEEP).await;
        refuses_unchanged(DEEP_KEY, source, true, POSITIONAL, ALTER_DEEP).await;
        refuses_unchanged(DEEP_KEY, source, false, EVOLVING_MERGE, ALTER_DEEP).await;
    }
}

#[tokio::test]
async fn a_union_that_adds_a_field_to_a_map_key_struct_refuses_with_the_add_text() {
    for source in [
        "mk MAP<STRUCT<s: STRUCT<x: INT>, a: INT, c: INT>, INT>",
        "mk MAP<STRUCT<s: STRUCT<x: INT, z: INT>, a: INT, c: INT>, INT>",
    ] {
        refuses_unchanged(DEEP_KEY, source, true, BY_NAME, ADD_DEEP).await;
        refuses_unchanged(DEEP_KEY, source, false, EVOLVING_MERGE, ADD_DEEP).await;
    }
}

#[tokio::test]
async fn a_union_that_widens_a_primitive_map_key_refuses_with_the_update_text() {
    let expected = "Unsupported table change: Cannot update map keys: map<int, int>";
    for sql in [BY_NAME, EVOLVING_MERGE] {
        refuses_unchanged(
            "mk MAP<INT, INT>",
            "mk MAP<BIGINT, INT>",
            true,
            sql,
            expected,
        )
        .await;
    }
}

#[tokio::test]
async fn a_dataframe_append_with_an_extra_column_and_a_key_change_adds_nothing() {
    let (wh, ctx, catalogs) = tables(
        DEEP_KEY,
        "mk MAP<STRUCT<s: STRUCT<x: INT, z: INT>, a: INT>, INT>, extra INT",
        true,
    )
    .await;
    let before = (target_state(&catalogs).await, target_files(&wh));
    let sql = "INSERT INTO ice.sales.t BY NAME SELECT `id`, `mk`, `extra` FROM ice.sales.src";
    let error = outcome(&ctx, &catalogs, sql).await.expect_err(sql);
    assert!(without_ids(&error).contains(ALTER_DEEP), "{error}");
    assert_eq!((target_state(&catalogs).await, target_files(&wh)), before);
}

#[tokio::test]
async fn map_value_and_list_element_unions_still_evolve() {
    for (target, source) in [
        (
            "m MAP<INT, STRUCT<x: INT>>",
            "m MAP<INT, STRUCT<x: INT, z: INT>>",
        ),
        (
            "ls ARRAY<STRUCT<x: INT>>",
            "ls ARRAY<STRUCT<x: INT, z: INT>>",
        ),
        (
            "mk MAP<STRUCT<a: INT>, STRUCT<x: INT>>",
            "mk MAP<STRUCT<a: INT>, STRUCT<x: INT, z: INT>>",
        ),
    ] {
        evolves(target, source, true, BY_NAME).await;
        evolves(target, source, false, EVOLVING_MERGE).await;
    }
}

#[tokio::test]
async fn a_union_that_leaves_the_map_key_as_it_is_still_writes() {
    let (_wh, ctx, catalogs) = tables(DEEP_KEY, DEEP_KEY, true).await;
    let before = target_state(&catalogs).await;
    for sql in [BY_NAME, EVOLVING_MERGE] {
        outcome(&ctx, &catalogs, sql).await.expect(sql);
        assert_eq!(target_state(&catalogs).await.1, before.1, "{sql}");
    }
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 1);
}
