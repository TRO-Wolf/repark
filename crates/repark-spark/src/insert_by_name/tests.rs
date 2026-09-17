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

fn named_verbatim(names: &[&str]) -> Vec<SourceName> {
    names
        .iter()
        .map(|name| SourceName {
            display: (*name).to_string(),
            resolved: (*name).to_string(),
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
    let mapping = match_source_to_target(
        &targets(),
        &named(&["last_name", "first_name", "n"]),
        TABLE,
        false,
    )
    .expect("maps");
    assert_eq!(mapping, vec![Some(1), Some(0), Some(2)]);
}

#[test]
fn match_subset_leaves_missing_slots_empty() {
    let mapping = match_source_to_target(&targets(), &named(&["first_name", "n"]), TABLE, false)
        .expect("maps");
    assert_eq!(mapping, vec![Some(0), None, Some(1)]);
}

#[test]
fn match_case_folds() {
    let mapping = match_source_to_target(
        &targets(),
        &named(&["FIRST_NAME", "Last_Name", "N"]),
        TABLE,
        false,
    )
    .expect("maps");
    assert_eq!(mapping, vec![Some(0), Some(1), Some(2)]);
}

#[test]
fn match_extra_reports_too_many_columns_byte_exact() {
    let error = match_source_to_target(
        &targets(),
        &named(&["first_name", "last_name", "n", "extra"]),
        TABLE,
        false,
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
        false,
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
        false,
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
    let error = match_source_to_target(&targets(), &named(&["first_name", "extra"]), TABLE, false)
        .expect_err("shorter extra refuses");
    assert_eq!(
        error.to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] Cannot write incompatible data for the \
         table `sc`.`ns`.`bn`: Cannot write extra columns `extra`. SQLSTATE: KD000"
    );
}

#[test]
fn match_values_names_report_extra_columns() {
    let error = match_source_to_target(&targets(), &named(&["col1", "col2", "col3"]), TABLE, false)
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
    assert_eq!(
        build_projection_sql(
            &source,
            &targets(),
            &named(&["last_name", "first_name", "n"]),
            &[
                TargetFill::Source(1),
                TargetFill::Null,
                TargetFill::Source(2),
            ]
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
    let names = syntactic_source_names(&source, false).expect("syntactic");
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
    assert!(syntactic_source_names(&source, false).is_none());
}

#[test]
fn match_case_sensitive_keeps_exact_and_reports_extra() {
    let mapping = match_source_to_target(
        &targets(),
        &named_verbatim(&["first_name", "n"]),
        TABLE,
        true,
    )
    .expect("exact maps");
    assert_eq!(mapping, vec![Some(0), None, Some(1)]);
    let error = match_source_to_target(
        &targets(),
        &named_verbatim(&["N", "FIRST_NAME"]),
        TABLE,
        true,
    )
    .expect_err("case mismatch refuses");
    assert_eq!(
        error.to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] Cannot write incompatible data for \
         the table `sc`.`ns`.`bn`: Cannot write extra columns `N`, `FIRST_NAME`. SQLSTATE: KD000"
    );
}

#[test]
fn match_case_sensitive_dup_after_fold_stays_distinct() {
    let mapping = match_source_to_target(
        &targets(),
        &named_verbatim(&["first_name", "FIRST_NAME", "last_name"]),
        TABLE,
        true,
    )
    .expect_err("distinct spellings refuse as extra");
    assert!(mapping.to_string().contains("EXTRA_COLUMNS"), "{mapping}");
}

#[test]
fn static_partition_column_in_list_names_the_clause_spelling() {
    let error = static_partition_in_column_list("p");
    assert_eq!(
        error.to_string(),
        "Error during planning: [STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST] Static partition column p is \
         also specified in the column list. SQLSTATE: 42713"
    );
}

#[test]
fn cannot_find_data_names_the_missing_required_column() {
    let error = cannot_find_data(TABLE, "last_name");
    assert_eq!(
        error.to_string(),
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA] Cannot write incompatible data for \
         the table `sc`.`ns`.`bn`: Cannot find data for the output column `last_name`. SQLSTATE: KD000"
    );
}

#[test]
fn partition_literal_sql_renders_each_kind() {
    use repark_iceberg::write::PartitionLiteral;
    assert_eq!(partition_literal_sql(&None), "NULL");
    assert_eq!(
        partition_literal_sql(&Some(PartitionLiteral::Boolean(true))),
        "TRUE"
    );
    assert_eq!(
        partition_literal_sql(&Some(PartitionLiteral::Boolean(false))),
        "FALSE"
    );
    assert_eq!(partition_literal_sql(&Some(PartitionLiteral::Int(1))), "1");
    assert_eq!(partition_literal_sql(&Some(PartitionLiteral::Long(9))), "9");
    assert_eq!(
        partition_literal_sql(&Some(PartitionLiteral::String("o'x".to_string()))),
        "'o''x'"
    );
}

#[test]
fn strip_insert_into_partition_by_name_keeps_the_clause() {
    assert_eq!(
        strip_insert_by_name("INSERT INTO t PARTITION (p = 1) BY NAME SELECT a FROM s")
            .expect("strips"),
        Some("INSERT INTO t PARTITION (p = 1)  SELECT a FROM s".to_string())
    );
}

#[test]
fn syntactic_names_keep_verbatim_under_case_sensitive() {
    let source = parse_query("SELECT 'Di' AS FIRST_NAME, n FROM t");
    let names = syntactic_source_names(&source, true).expect("syntactic");
    assert_eq!(
        names
            .iter()
            .map(|name| (name.display.as_str(), name.resolved.as_str()))
            .collect::<Vec<_>>(),
        vec![("FIRST_NAME", "FIRST_NAME"), ("n", "n")]
    );
}

#[test]
fn parse_projection_query_round_trips() {
    let query = parse_projection_query(
        "SELECT `b` AS `a`, NULL AS `c` FROM (SELECT 1 AS b) AS _repark_by_name_src",
    )
    .expect("parses");
    assert!(matches!(query.body.as_ref(), SetExpr::Select(_)));
}
