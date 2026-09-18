use super::super::*;
use super::common::*;
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::util::display::array_value_to_string;
use iceberg::spec::{NestedFieldRef, Type};
use tempfile::TempDir;

const NESTED_CREATE: &str = "CREATE TABLE ice.sales.nested (id INT, s STRUCT<a: INT, b: STRING>, \
     arrs ARRAY<STRUCT<x: INT>>, m MAP<STRING, STRUCT<p: INT>>) USING iceberg";

const STRUCT_MAP_CREATE: &str = "CREATE TABLE ice.sales.sm (id INT, s STRUCT<a: INT, b: STRING>, \
     m MAP<STRING, STRUCT<p: INT>>) USING iceberg";

const STRUCT_MAP_INSERT: &str = "INSERT INTO ice.sales.sm SELECT 1, named_struct('a', 1, 'b', 'p'), \
     map(['k'], [named_struct('p', 1)])";

async fn collect(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<RecordBatch> {
    execute(ctx, catalogs, sql)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap()
}

async fn rendered_row(ctx: &SessionContext, catalogs: &CatalogRegistry, sql: &str) -> Vec<String> {
    let batches = collect(ctx, catalogs, sql).await;
    (0..batches[0].num_columns())
        .map(|column| array_value_to_string(batches[0].column(column).as_ref(), 0).unwrap())
        .collect()
}

async fn describe_types(ctx: &SessionContext, catalogs: &CatalogRegistry) -> Vec<String> {
    let batches = collect(ctx, catalogs, "DESCRIBE TABLE ice.sales.nested").await;
    let mut types = Vec::new();
    for batch in &batches {
        let names = batch.column(0).as_string::<i32>();
        let kinds = batch.column(1).as_string::<i32>();
        for row in 0..batch.num_rows() {
            types.push(format!("{} {}", names.value(row), kinds.value(row)));
        }
    }
    types
}

fn struct_children(field: &NestedFieldRef) -> Vec<(i32, String, bool)> {
    let children = match field.field_type.as_ref() {
        Type::Struct(inner) => inner.fields().to_vec(),
        Type::List(list) => match list.element_field.field_type.as_ref() {
            Type::Struct(inner) => inner.fields().to_vec(),
            other => panic!("list element is not a struct: {other}"),
        },
        Type::Map(map) => match map.value_field.field_type.as_ref() {
            Type::Struct(inner) => inner.fields().to_vec(),
            other => panic!("map value is not a struct: {other}"),
        },
        other => panic!("not a nested type: {other}"),
    };
    children
        .iter()
        .map(|child| (child.id, child.name.clone(), child.required))
        .collect()
}

async fn children_of(catalogs: &CatalogRegistry, column: &str) -> Vec<(i32, String, bool)> {
    let table = load_sales_table(catalogs, "nested").await;
    let schema = table.metadata().current_schema();
    let field = schema.field_by_name(column).unwrap().clone();
    struct_children(&field)
}

#[tokio::test]
async fn nested_create_round_trips_the_describe_types() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, NESTED_CREATE).await;
    assert_eq!(
        describe_types(&ctx, &catalogs).await,
        vec![
            "id int".to_string(),
            "s struct<a:int,b:string>".to_string(),
            "arrs array<struct<x:int>>".to_string(),
            "m map<string,struct<p:int>>".to_string(),
        ]
    );
}

#[tokio::test]
async fn nested_struct_and_map_insert_reads_back() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, STRUCT_MAP_CREATE).await;
    run(&ctx, &catalogs, STRUCT_MAP_INSERT).await;
    assert_eq!(
        rendered_row(&ctx, &catalogs, "SELECT s, m FROM ice.sales.sm").await,
        vec!["{a: 1, b: p}", "{k: {p: 1}}"]
    );
}

#[tokio::test]
#[ignore = "fork finding: an INSERT into a list column fails in the fork writer"]
async fn forkwrite_list_insert_reads_back() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, NESTED_CREATE).await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.nested SELECT 1, named_struct('a', 1, 'b', 'p'), \
         make_array(named_struct('x', 1)), map(['k'], [named_struct('p', 1)])",
    )
    .await;
    assert_eq!(
        rendered_row(&ctx, &catalogs, "SELECT arrs FROM ice.sales.nested").await,
        vec!["[{x: 1}]"]
    );
}

#[tokio::test]
async fn nested_add_rename_and_drop_evolve_by_field_id() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, NESTED_CREATE).await;
    let before = children_of(&catalogs, "s").await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMN s.c BIGINT",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMN arrs.element.y INT",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMNS (m.value.q STRING)",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested RENAME COLUMN s.a TO a2",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested DROP COLUMN s.b",
    )
    .await;
    let after = children_of(&catalogs, "s").await;
    assert_eq!(after[0], (before[0].0, "a2".to_string(), false));
    assert_eq!(after.len(), 2);
    assert_eq!(after[1].1, "c");
    assert!(!after[1].2);
    let element: Vec<String> = children_of(&catalogs, "arrs")
        .await
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    assert_eq!(element, vec!["x", "y"]);
    let value: Vec<String> = children_of(&catalogs, "m")
        .await
        .into_iter()
        .map(|(_, name, _)| name)
        .collect();
    assert_eq!(value, vec!["p", "q"]);
    assert_eq!(
        describe_types(&ctx, &catalogs).await,
        vec![
            "id int".to_string(),
            "s struct<a2:int,c:bigint>".to_string(),
            "arrs array<struct<x:int,y:int>>".to_string(),
            "m map<string,struct<p:int,q:string>>".to_string(),
        ]
    );
}

#[tokio::test]
async fn nested_required_child_refuses_and_keeps_the_schema() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, NESTED_CREATE).await;
    let schema_id = load_sales_table(&catalogs, "nested")
        .await
        .metadata()
        .current_schema_id();
    let refused = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMN s.r INT NOT NULL",
    )
    .await
    .expect_err("a required nested child without a default must refuse");
    assert!(
        refused
            .to_string()
            .contains("Incompatible change: cannot add required column"),
        "{refused}"
    );
    let table = load_sales_table(&catalogs, "nested").await;
    assert_eq!(table.metadata().current_schema_id(), schema_id);
}

#[tokio::test]
async fn nested_ddl_refuses_malformed_paths_spark_shaped() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, NESTED_CREATE).await;
    for sql in [
        "ALTER TABLE ice.sales.nested ADD COLUMN s.c INT AFTER s.a",
        "ALTER TABLE ice.sales.nested RENAME COLUMN s.a TO a2 EXTRA",
    ] {
        let refused = execute(&ctx, &catalogs, sql).await.expect_err(sql);
        assert!(
            refused.to_string().contains("[PARSE_SYNTAX_ERROR]"),
            "{sql}: {refused}"
        );
    }
    let dangling = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested DROP COLUMN s.b,",
    )
    .await
    .expect_err("a dangling comma must refuse");
    assert!(matches!(dangling, DataFusionError::SQL(_, _)), "{dangling}");
    let missing = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMN nope.c INT",
    )
    .await
    .expect_err("an unknown parent must refuse");
    assert!(missing.to_string().contains("nope"), "{missing}");
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested DROP COLUMN IF EXISTS s.nope",
    )
    .await;
}

#[tokio::test]
async fn nested_ddl_refuses_double_quoted_names_and_known_paths_spark_shaped() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, NESTED_CREATE).await;
    let schema_id = load_sales_table(&catalogs, "nested")
        .await
        .metadata()
        .current_schema_id();
    for sql in [
        "ALTER TABLE ice.sales.nested RENAME COLUMN s.a TO \"x.y\"",
        "ALTER TABLE ice.sales.nested ADD COLUMN s.\"x.y\" INT",
    ] {
        let refused = execute(&ctx, &catalogs, sql).await.expect_err(sql);
        assert!(
            matches!(&refused, DataFusionError::Context(_, inner) if matches!(**inner, DataFusionError::SQL(_, _))),
            "{sql}: {refused}"
        );
        assert!(
            refused.to_string().contains(
                "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"x.y\"'. SQLSTATE: 42601"
            ),
            "{sql}: {refused}"
        );
    }
    let duplicate = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMN s.A INT",
    )
    .await
    .expect_err("an existing child must refuse");
    assert!(matches!(duplicate, DataFusionError::Plan(_)), "{duplicate}");
    assert!(
        duplicate.to_string().contains(
            "[FIELD_ALREADY_EXISTS] Cannot add column, because `s`.`A` already exists in \
             \"STRUCT<id: INT, s: STRUCT<a: INT, b: STRING>, arrs: ARRAY<STRUCT<x: INT>>, \
             m: MAP<STRING, STRUCT<p: INT>>>\". SQLSTATE: 42710"
        ),
        "{duplicate}"
    );
    let unknown = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.nested ADD COLUMN nope.c INT",
    )
    .await
    .expect_err("an unknown parent must refuse");
    assert!(matches!(unknown, DataFusionError::Plan(_)), "{unknown}");
    assert!(
        unknown.to_string().contains(
            "[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function \
                parameter with name `nope` cannot be resolved. Did you mean one of the \
                following? [`id`, `s`, `arrs`, `m`]. SQLSTATE: 42703"
        ),
        "{unknown}"
    );
    let table = load_sales_table(&catalogs, "nested").await;
    assert_eq!(table.metadata().current_schema_id(), schema_id);
}

#[tokio::test]
async fn nested_create_not_null_child_is_required_with_level_order_ids() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.nested (id INT, s STRUCT<a: INT NOT NULL, b: STRUCT<c: INT>>, \
         m MAP<STRING, STRUCT<q: INT NOT NULL>>) USING iceberg",
    )
    .await;
    assert_eq!(
        children_of(&catalogs, "s").await,
        vec![(4, "a".to_string(), true), (5, "b".to_string(), false)]
    );
    assert_eq!(
        children_of(&catalogs, "m").await,
        vec![(9, "q".to_string(), true)]
    );
    let refused = execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.opt (s STRUCT<a: INT OPTIONS(repark_not_null = TRUE)>) USING iceberg",
    )
    .await
    .expect_err("a struct-field OPTIONS clause is not Spark syntax");
    assert!(
        refused
            .to_string()
            .contains("[PARSE_SYNTAX_ERROR] Syntax error at or near 'OPTIONS'. SQLSTATE: 42601"),
        "{refused}"
    );
}

#[test]
fn nested_parse_leaves_top_level_forms_to_the_existing_path() {
    for sql in [
        "ALTER TABLE ice.sales.t ADD COLUMN c STRING",
        "ALTER TABLE ice.sales.t ADD COLUMNS (c STRING, d INT)",
        "ALTER TABLE ice.sales.t DROP COLUMN c",
        "ALTER TABLE ice.sales.t RENAME COLUMN c TO d",
        "ALTER TABLE ice.sales.t ALTER COLUMN s.b FIRST",
        "SELECT s.a FROM ice.sales.t",
    ] {
        assert!(
            crate::nested_column_ddl::try_parse_nested_column_ddl(sql).is_none(),
            "{sql}"
        );
    }
    for sql in [
        "ALTER TABLE ice.sales.t ADD COLUMN s.c STRING",
        "ALTER TABLE ice.sales.t ADD COLUMNS s.c STRING COMMENT 'x' FIRST, d INT",
        "ALTER TABLE ice.sales.t ADD COLUMN m.value.q MAP<STRING, STRUCT<z: INT>>",
        "ALTER TABLE ice.sales.t DROP COLUMNS (s.b, c)",
        "ALTER TABLE ice.sales.t RENAME COLUMN s.a TO a2",
        "ALTER TABLE ice.sales.t ADD COLUMN s.d INT COMMENT \"x.y\" FIRST",
    ] {
        assert!(
            matches!(
                crate::nested_column_ddl::try_parse_nested_column_ddl(sql),
                Some(Ok(_))
            ),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn fork292_nested_add_reads_null_for_rows_written_before() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, STRUCT_MAP_CREATE).await;
    run(&ctx, &catalogs, STRUCT_MAP_INSERT).await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sm ADD COLUMN s.c BIGINT",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sm ADD COLUMN m.value.q STRING",
    )
    .await;
    let batches = collect(&ctx, &catalogs, "SELECT s FROM ice.sales.sm").await;
    assert!(batches[0].column(0).as_struct().column(2).is_null(0));
    assert_eq!(
        rendered_row(&ctx, &catalogs, "SELECT s, m FROM ice.sales.sm").await,
        vec!["{a: 1, b: p, c: }", "{k: {p: 1, q: }}"]
    );
}

#[tokio::test]
async fn nested_add_comment_takes_a_double_quoted_string_spark_shaped() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, STRUCT_MAP_CREATE).await;
    for sql in [
        "ALTER TABLE ice.sales.sm ADD COLUMN s.d INT COMMENT \"x.y\"",
        "ALTER TABLE ice.sales.sm ADD COLUMN s.e INT COMMENT \"c\"",
        "ALTER TABLE ice.sales.sm ADD COLUMN s.f INT COMMENT \"x.y\" FIRST",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    let table = load_sales_table(&catalogs, "sm").await;
    let schema = table.metadata().current_schema();
    let field = schema.field_by_name("s").unwrap();
    let Type::Struct(inner) = field.field_type.as_ref() else {
        panic!("s is not a struct");
    };
    let docs: Vec<(&str, Option<&str>)> = inner
        .fields()
        .iter()
        .map(|child| (child.name.as_str(), child.doc.as_deref()))
        .collect();
    assert_eq!(
        docs,
        vec![
            ("f", Some("x.y")),
            ("a", None),
            ("b", None),
            ("d", Some("x.y")),
            ("e", Some("c")),
        ]
    );
}
