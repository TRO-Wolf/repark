use super::*;

fn targets() -> Vec<String> {
    ["first_name", "last_name", "n"]
        .iter()
        .map(ToString::to_string)
        .collect()
}

fn named(names: &[&str]) -> Vec<SourceName> {
    names
        .iter()
        .map(|name| SourceName {
            display: (*name).to_string(),
            resolved: (*name).to_ascii_lowercase(),
        })
        .collect()
}

const TABLE: &str = "`sc`.`ns`.`bn`";

#[test]
fn strip_plain_insert_by_name() {
    assert_eq!(
        strip_insert_by_name("INSERT INTO sc.ns.bn BY NAME SELECT * FROM s").expect("strips"),
        Some("INSERT INTO sc.ns.bn  SELECT * FROM s".to_string())
    );
}

#[test]
fn strip_overwrite_and_table_keyword_and_case() {
    assert_eq!(
        strip_insert_by_name("insert overwrite table sc.ns.bn by name select 1").expect("strips"),
        Some("insert overwrite table sc.ns.bn  select 1".to_string())
    );
}

#[test]
fn strip_absent_without_by_name() {
    assert_eq!(
        strip_insert_by_name("INSERT INTO sc.ns.bn SELECT * FROM s").expect("parses"),
        None
    );
    assert_eq!(
        strip_insert_by_name("SELECT * FROM s ORDER BY name").expect("parses"),
        None
    );
    assert_eq!(
        strip_insert_by_name("INSERT INTO sc.ns.bn SELECT * FROM s ORDER BY name").expect("parses"),
        None
    );
}

#[test]
fn strip_absent_for_quoted_by_name() {
    assert_eq!(
        strip_insert_by_name("INSERT INTO sc.ns.bn SELECT \"BY\" FROM s").expect("parses"),
        None
    );
}

#[test]
fn strip_cte_and_parenthesized_sources() {
    assert_eq!(
        strip_insert_by_name("INSERT INTO t BY NAME WITH c AS (SELECT 1 AS a) SELECT * FROM c")
            .expect("strips"),
        Some("INSERT INTO t  WITH c AS (SELECT 1 AS a) SELECT * FROM c".to_string())
    );
    assert_eq!(
        strip_insert_by_name("INSERT INTO t BY NAME (SELECT a FROM s)").expect("strips"),
        Some("INSERT INTO t  (SELECT a FROM s)".to_string())
    );
}

#[test]
fn strip_column_list_by_name_is_parse_error() {
    let error = strip_insert_by_name("INSERT INTO sc.ns.bn (n, first_name) BY NAME SELECT 1 AS n")
        .expect_err("column list plus BY NAME refuses");
    assert!(error.to_string().contains("PARSE_SYNTAX_ERROR"), "{error}");
    assert!(error.to_string().contains("BY NAME"), "{error}");
    assert!(matches!(error, DataFusionError::SQL(_, _)), "{error}");
}

#[test]
fn strip_partition_by_name_strips_for_downstream_routing() {
    assert_eq!(
        strip_insert_by_name("INSERT OVERWRITE t PARTITION (p = 1) BY NAME SELECT a FROM s")
            .expect("strips"),
        Some("INSERT OVERWRITE t PARTITION (p = 1)  SELECT a FROM s".to_string())
    );
}

#[test]
fn match_full_reorder_maps_positions() {
    let mapping =
        match_source_to_target(&targets(), &named(&["last_name", "first_name", "n"]), TABLE)
            .expect("maps");
    assert_eq!(mapping, vec![Some(1), Some(0), Some(2)]);
}

#[test]
fn match_subset_leaves_missing_slots_empty() {
    let mapping =
        match_source_to_target(&targets(), &named(&["first_name", "n"]), TABLE).expect("maps");
    assert_eq!(mapping, vec![Some(0), None, Some(1)]);
}

#[test]
fn match_case_folds() {
    let mapping =
        match_source_to_target(&targets(), &named(&["FIRST_NAME", "Last_Name", "N"]), TABLE)
            .expect("maps");
    assert_eq!(mapping, vec![Some(0), Some(1), Some(2)]);
}

#[test]
fn match_extra_reports_too_many_columns_byte_exact() {
    let error = match_source_to_target(
        &targets(),
        &named(&["first_name", "last_name", "n", "extra"]),
        TABLE,
    )
    .expect_err("extra refuses");
    assert_eq!(
        error.to_string(),
        "Error during planning: [INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to `sc`.`ns`.`bn`, \
         the reason is too many data columns:\nTable columns: `first_name`, `last_name`, `n`.\nData \
         columns: `first_name`, `last_name`, `n`, `extra`. SQLSTATE: 21S01"
    );
}

#[test]
fn match_longer_partial_overlap_reports_too_many() {
    let error = match_source_to_target(
        &targets(),
        &named(&["first_name", "bogus1", "bogus2", "bogus3"]),
        TABLE,
    )
    .expect_err("longer partial refuses");
    assert!(
        error.to_string().contains("TOO_MANY_DATA_COLUMNS"),
        "{error}"
    );
}

#[test]
fn match_duplicate_reports_ambiguous_byte_exact() {
    let error = match_source_to_target(
        &targets(),
        &named(&["first_name", "FIRST_NAME", "last_name"]),
        TABLE,
    )
    .expect_err("case duplicate refuses");
    assert_eq!(
        error.to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.AMBIGUOUS_COLUMN_NAME] Cannot write incompatible data for \
         the table `sc`.`ns`.`bn`: Ambiguous column name in the input data `first_name`. \
         SQLSTATE: KD000"
    );
}

#[test]
fn match_shorter_extra_reports_extra_columns_byte_exact() {
    let error = match_source_to_target(&targets(), &named(&["first_name", "extra"]), TABLE)
        .expect_err("shorter extra refuses");
    assert_eq!(
        error.to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] Cannot write incompatible data for the \
         table `sc`.`ns`.`bn`: Cannot write extra columns `extra`. SQLSTATE: KD000"
    );
}

#[test]
fn match_values_names_report_extra_columns() {
    let error = match_source_to_target(&targets(), &named(&["col1", "col2", "col3"]), TABLE)
        .expect_err("values names refuse");
    assert!(
        error.to_string().contains("`col1`, `col2`, `col3`"),
        "{error}"
    );
}

#[test]
fn build_projection_sql_reorders_and_null_fills() {
    let source: Box<Query> = Parser::parse_sql(
        &DatabricksDialect {},
        "SELECT last_name, first_name, n FROM s",
    )
    .expect("parses")
    .pop()
    .and_then(|statement| match statement {
        Statement::Query(query) => Some(query),
        _ => None,
    })
    .expect("query");
    let mapping = vec![Some(1), None, Some(2)];
    assert_eq!(
        build_projection_sql(
            &source,
            &targets(),
            &named(&["last_name", "first_name", "n"]),
            &mapping
        ),
        "SELECT `first_name` AS `first_name`, NULL AS `last_name`, `n` AS `n` FROM \
         (SELECT last_name, first_name, n FROM s) AS _repark_by_name_src"
    );
}

fn parse_query(sql: &str) -> Box<Query> {
    Parser::parse_sql(&DatabricksDialect {}, sql)
        .expect("parses")
        .pop()
        .and_then(|statement| match statement {
            Statement::Query(query) => Some(query),
            _ => None,
        })
        .expect("query")
}

#[test]
fn syntactic_names_keep_display_and_fold_unquoted() {
    let source = parse_query("SELECT 'Di' AS FIRST_NAME, t.Last_Name, n FROM t");
    let names = syntactic_source_names(&source).expect("syntactic");
    assert_eq!(
        names
            .iter()
            .map(|name| (name.display.as_str(), name.resolved.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("FIRST_NAME", "first_name"),
            ("Last_Name", "last_name"),
            ("n", "n")
        ]
    );
}

#[test]
fn syntactic_names_decline_stars() {
    let source = parse_query("SELECT * FROM t");
    assert!(syntactic_source_names(&source).is_none());
}

#[test]
fn parse_projection_query_round_trips() {
    let query = parse_projection_query(
        "SELECT `b` AS `a`, NULL AS `c` FROM (SELECT 1 AS b) AS _repark_by_name_src",
    )
    .expect("parses");
    assert!(matches!(query.body.as_ref(), SetExpr::Select(_)));
}
