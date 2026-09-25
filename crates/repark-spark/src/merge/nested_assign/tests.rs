use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{Assignment, AssignmentTarget, Statement};
use datafusion::sql::sqlparser::dialect::DatabricksDialect;
use datafusion::sql::sqlparser::parser::Parser;

use super::{
    AssignmentScope, Step, fold_nested_assignments, render, resolve_key, resolve_target,
    value_sql_for,
};

fn struct_type(fields: Vec<Field>) -> DataType {
    DataType::Struct(Fields::from(fields))
}

fn schema() -> ArrowSchema {
    let inner = struct_type(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    ArrowSchema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new(
            "st",
            struct_type(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("inner", inner, true),
            ]),
            true,
        ),
        Field::new(
            "arr",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, true))),
            true,
        ),
    ])
}

fn scope() -> AssignmentScope {
    AssignmentScope {
        qualifiers: vec![vec!["t".to_string()]],
        sql_qualifier: "t".to_string(),
        column_prefix: Some("t".to_string()),
        value_qualifiers: vec![vec!["t".to_string()], vec!["s".to_string()]],
        probe_from: "missing_probe_table".to_string(),
        case_sensitive: false,
    }
}

fn parts(path: &str) -> Vec<String> {
    path.split('.').map(ToString::to_string).collect()
}

fn assignments(sql: &str) -> Vec<Assignment> {
    let statement = Parser::new(&DatabricksDialect {})
        .try_with_sql(sql)
        .and_then(|mut parser| parser.parse_statement())
        .unwrap();
    let Statement::Update(update) = statement else {
        panic!("expected an UPDATE");
    };
    update.assignments
}

#[test]
fn a_struct_path_resolves_to_canonical_field_names() {
    let key = resolve_key(&schema(), &parts("ST.Inner.X"), false)
        .unwrap()
        .unwrap();
    assert_eq!(key.column, "st");
    assert_eq!(
        key.steps,
        vec![
            Step::Field("inner".to_string()),
            Step::Field("x".to_string())
        ]
    );
    assert_eq!(key.pretty(), "st.inner.x");
    assert_eq!(key.sql(&scope()), "t.st.`inner`.`x`");
}

#[test]
fn an_unknown_column_is_left_to_the_existing_path() {
    assert!(
        resolve_key(&schema(), &parts("zz.a"), false)
            .unwrap()
            .is_none()
    );
}

#[test]
fn key_resolution_refuses_as_spark_does() {
    let missing = resolve_key(&schema(), &parts("st.zz"), false)
        .unwrap_err()
        .to_string();
    assert_eq!(
        missing,
        "Error during planning: [FIELD_NOT_FOUND] No such struct field `zz` in `a`, `inner`. \
         SQLSTATE: 42704"
    );
    let atomic = resolve_key(&schema(), &parts("id.a"), false)
        .unwrap_err()
        .to_string();
    assert_eq!(
        atomic,
        "Error during planning: [INVALID_EXTRACT_BASE_FIELD_TYPE] Can't extract a value from \
         \"id\". Need a complex type [STRUCT, ARRAY, MAP] but got \"BIGINT\". SQLSTATE: 42000"
    );
    let index = resolve_key(&schema(), &parts("arr.b"), false)
        .unwrap_err()
        .to_string();
    assert_eq!(
        index,
        "Error during planning: [DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve \
         \"arr[b]\" due to data type mismatch: The second parameter requires the \"INTEGRAL\" \
         type, however \"b\" has the type \"STRING\". SQLSTATE: 42K09"
    );
}

fn update_scope(case_sensitive: bool) -> AssignmentScope {
    AssignmentScope {
        qualifiers: vec![
            vec!["db".to_string(), "tbl".to_string()],
            vec!["tbl".to_string()],
        ],
        sql_qualifier: "db.tbl".to_string(),
        column_prefix: None,
        value_qualifiers: Vec::new(),
        probe_from: "missing_probe_table".to_string(),
        case_sensitive,
    }
}

fn target_answer(scope: &AssignmentScope, sql: &str) -> String {
    let assignment = assignments(&format!("UPDATE t SET {sql} = 5")).remove(0);
    let AssignmentTarget::ColumnName(name) = &assignment.target else {
        panic!("expected a column target");
    };
    match resolve_target(&schema(), scope, name) {
        Ok(Some(key)) => format!("{}.{}", key.column, key.pretty()),
        Ok(None) => "none".to_string(),
        Err(error) => error.to_string(),
    }
}

#[test]
fn a_case_sensitive_session_resolves_set_keys_exactly_as_spark_does() {
    let merge = AssignmentScope {
        case_sensitive: true,
        ..scope()
    };
    assert_eq!(
        target_answer(&merge, "t.st.A"),
        "Error during planning: [FIELD_NOT_FOUND] No such struct field `A` in `a`, `inner`. \
         SQLSTATE: 42704"
    );
    assert_eq!(
        target_answer(&merge, "t.ST.a"),
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `t`.`ST`.`a` cannot be resolved. Did you mean one of the \
         following? [`id`, `st`, `arr`]. SQLSTATE: 42703"
    );
    assert_eq!(
        target_answer(&merge, "ID"),
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `ID` cannot be resolved. Did you mean one of the \
         following? [`id`, `st`, `arr`]. SQLSTATE: 42703"
    );
    assert_eq!(target_answer(&merge, "t.st.a"), "st.st.a");
    assert_eq!(target_answer(&merge, "zz"), "none");
    assert_eq!(
        target_answer(&update_scope(true), "ST"),
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `ST` cannot be resolved. Did you mean one of the \
         following? [`id`, `st`, `arr`]. SQLSTATE: 42703"
    );
    assert_eq!(
        target_answer(&update_scope(true), "ST.a"),
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `ST`.`a` cannot be resolved. Did you mean one of the \
         following? [`tbl`.`id`, `tbl`.`st`, `tbl`.`arr`]. SQLSTATE: 42703"
    );
    assert_eq!(target_answer(&scope(), "t.ST.A"), "st.st.a");
    assert_eq!(target_answer(&scope(), "ID"), "id.id");
    assert_eq!(target_answer(&update_scope(false), "ST"), "st.st");
}

#[test]
fn scala_type_names_follow_spark_to_string() {
    let map = DataType::Map(
        Arc::new(Field::new(
            "key_value",
            struct_type(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    struct_type(vec![Field::new("x", DataType::Int32, true)]),
                    true,
                ),
            ]),
            false,
        )),
        false,
    );
    assert_eq!(
        render::scala_type(&map),
        "MapType(StringType,StructType(StructField(x,IntegerType,true)),true)"
    );
    assert_eq!(
        render::scala_type(&DataType::List(Arc::new(Field::new(
            "element",
            DataType::Decimal128(10, 2),
            false
        )))),
        "ArrayType(DecimalType(10,2),false)"
    );
}

#[test]
fn a_path_part_is_quoted_only_when_spark_quotes_it() {
    assert_eq!(render::quote_if_needed("st"), "st");
    assert_eq!(render::quote_if_needed("b c"), "`b c`");
    assert_eq!(render::quote_if_needed("12"), "`12`");
    assert_eq!(render::quote_if_needed("a`b"), "`a``b`");
}

#[test]
fn pretty_values_drop_qualifiers_and_string_quotes() {
    let values = assignments(
        "UPDATE t SET a = named_struct('a', s.na, 'b', 'z'), b = t.st.a + 1, c = sc.ns.t.id",
    );
    let qualifiers = vec![
        vec!["t".to_string()],
        vec!["s".to_string()],
        vec!["sc".to_string(), "ns".to_string(), "t".to_string()],
    ];
    let pretty: Vec<String> = values
        .iter()
        .map(|assignment| render::pretty_expr(&assignment.value, &qualifiers))
        .collect();
    assert_eq!(pretty, ["named_struct(a, na, b, z)", "(st.a + 1)", "id"]);
}

#[test]
fn a_struct_value_resolves_by_name_as_spark_does() {
    let target = struct_type(vec![
        Field::new("x", DataType::Int32, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    let path = parts("st.inner");
    let missing = struct_type(vec![
        Field::new("q", DataType::Int64, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    assert_eq!(
        value_sql_for("v", &missing, &target, &path, false)
            .unwrap_err()
            .to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write \
         incompatible data for the table ``: Cannot find data for the output column \
         `st`.`inner`.`x`. SQLSTATE: KD000"
    );
    let extra = struct_type(vec![
        Field::new("x", DataType::Int64, true),
        Field::new("y", DataType::Utf8, true),
        Field::new("z", DataType::Int64, true),
    ]);
    assert_eq!(
        value_sql_for("v", &extra, &target, &path, false)
            .unwrap_err()
            .to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Cannot write \
         incompatible data for the table ``: Cannot write extra fields `z` to the struct \
         `st`.`inner`. SQLSTATE: KD000"
    );
    let bad_leaf = struct_type(vec![
        Field::new("x", DataType::Utf8, true),
        Field::new("y", DataType::Utf8, true),
    ]);
    assert!(
        value_sql_for("v", &bad_leaf, &target, &path, false)
            .unwrap_err()
            .to_string()
            .contains("Cannot safely cast `st`.`inner`.`x` \"STRING\" to \"INT\"")
    );
    let reordered = struct_type(vec![
        Field::new("y", DataType::Utf8, true),
        Field::new("x", DataType::Int64, true),
    ]);
    assert_eq!(
        value_sql_for("v", &reordered, &target, &path, false).unwrap(),
        "arrow_cast((v), 'Struct(\"x\": Int32, \"y\": Utf8)')"
    );
    let recased = struct_type(vec![
        Field::new("X", DataType::Int64, true),
        Field::new("Y", DataType::Utf8, true),
    ]);
    assert_eq!(
        value_sql_for("v", &recased, &target, &path, false).unwrap(),
        "CASE WHEN (v) IS NULL THEN NULL ELSE named_struct('x', \
         arrow_cast((get_field((v), 'X')), 'Int32'), 'y', \
         arrow_cast((get_field((v), 'Y')), 'Utf8')) END"
    );
    assert_eq!(
        value_sql_for("v", &recased, &target, &path, true)
            .unwrap_err()
            .to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write \
         incompatible data for the table ``: Cannot find data for the output column \
         `st`.`inner`.`x`. SQLSTATE: KD000"
    );
    let required = struct_type(vec![
        Field::new("x", DataType::Int32, false),
        Field::new("y", DataType::Utf8, true),
    ]);
    assert_eq!(
        value_sql_for("v", &reordered, &required, &path, false).unwrap(),
        "CASE WHEN (v) IS NULL THEN NULL ELSE named_struct('x', \
         arrow_cast((get_field((v), 'x')), 'Int32'), 'y', \
         arrow_cast((get_field((v), 'y')), 'Utf8')) END"
    );
}

#[tokio::test]
async fn top_level_assignments_are_not_folded() {
    let ctx = SessionContext::new();
    let folded = fold_nested_assignments(
        &ctx,
        &schema(),
        &scope(),
        &assignments("UPDATE t SET t.id = 1, arr = NULL"),
    )
    .await
    .unwrap();
    assert!(folded.is_none());
    let repeated = fold_nested_assignments(
        &ctx,
        &schema(),
        &scope(),
        &assignments("UPDATE t SET t.id = 1, id = 2"),
    )
    .await
    .expect_err("a repeated top-level key refuses")
    .to_string();
    assert!(
        repeated.ends_with("- Multiple assignments for 'id': 1, 2 SQLSTATE: 42K09"),
        "{repeated}"
    );
}

#[tokio::test]
async fn a_top_level_struct_value_folds_through_the_by_name_check() {
    let ctx = SessionContext::new();
    let probed = AssignmentScope {
        probe_from: "(SELECT 1 AS one) s".to_string(),
        ..scope()
    };
    let fold = |sql: &'static str| {
        let ctx = ctx.clone();
        let probed = &probed;
        async move { fold_nested_assignments(&ctx, &schema(), probed, &assignments(sql)).await }
    };
    assert_eq!(
        fold("UPDATE t SET st = named_struct('q', 1, 'inner', named_struct('x', 1, 'y', 'v'))")
            .await
            .unwrap_err()
            .to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write \
         incompatible data for the table ``: Cannot find data for the output column `st`.`a`. \
         SQLSTATE: KD000"
    );
    assert_eq!(
        fold(
            "UPDATE t SET st = named_struct('a', 1, 'inner', named_struct('x', 1, 'y', 'v'), \
             'x', 2)"
        )
        .await
        .unwrap_err()
        .to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS] Cannot write \
         incompatible data for the table ``: Cannot write extra fields `x` to the struct `st`. \
         SQLSTATE: KD000"
    );
    assert_eq!(
        fold("UPDATE t SET st = named_struct('a', 1, 'inner', named_struct('x', 1)), id = 3")
            .await
            .unwrap_err()
            .to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write \
         incompatible data for the table ``: Cannot find data for the output column \
         `st`.`inner`.`y`. SQLSTATE: KD000"
    );
    let reordered = fold(
        "UPDATE t SET id = 3, st = named_struct('inner', named_struct('y', 'v', 'x', 3), 'a', 7)",
    )
    .await
    .unwrap()
    .unwrap();
    let rendered: Vec<String> = reordered.iter().map(ToString::to_string).collect();
    assert_eq!(rendered[0], "id = 3");
    assert_eq!(
        rendered[1],
        "`st` = arrow_cast((named_struct('inner', named_struct('y', 'v', 'x', 3), 'a', 7)), \
         'Struct(\"a\": Int32, \"inner\": Struct(\"x\": Int32, \"y\": Utf8))')"
    );
}

#[tokio::test]
async fn nested_assignments_fold_into_one_struct_rebuild() {
    let ctx = SessionContext::new();
    let folded = fold_nested_assignments(
        &ctx,
        &schema(),
        &scope(),
        &assignments("UPDATE t SET t.st.inner.x = s.v, id = 2, st.a = 7"),
    )
    .await
    .unwrap()
    .unwrap();
    let rendered: Vec<String> = folded.iter().map(ToString::to_string).collect();
    assert_eq!(
        rendered,
        [
            "`st` = named_struct('a', arrow_cast((7), 'Int32'), 'inner', \
             named_struct('x', arrow_cast((s.v), 'Int32'), 'y', \
             get_field(get_field(t.`st`, 'inner'), 'y')))",
            "id = 2",
        ]
    );
}

#[tokio::test]
async fn conflicting_and_repeated_assignments_refuse_with_every_error() {
    let ctx = SessionContext::new();
    let error = fold_nested_assignments(
        &ctx,
        &schema(),
        &scope(),
        &assignments("UPDATE t SET st.a = 1, st.a = 2, st.inner = NULL, st.inner.y = 'q', id = 3"),
    )
    .await
    .unwrap_err()
    .to_string();
    assert_eq!(
        error,
        "Error during planning: [DATATYPE_MISMATCH.INVALID_ROW_LEVEL_OPERATION_ASSIGNMENTS] \
         Cannot resolve \"st.a = 1\", \"st.a = 2\", \"st.inner = NULL\", \"st.inner.y = q\", \
         \"id = 3\" due to data type mismatch: \n- Multiple assignments for 'st.a': 1, 2\n- \
         Conflicting assignments for 'st.inner': t.st.`inner` = NULL, t.st.`inner`.`y` = 'q' \
         SQLSTATE: 42K09"
    );
}
