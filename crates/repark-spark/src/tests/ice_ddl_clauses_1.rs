use super::super::*;
use super::common::*;
use datafusion::sql::sqlparser::dialect::{GenericDialect, SparkSqlDialect};
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use iceberg::spec::Transform;
use repark_iceberg::write::alter::PartitionSpecChange;

use crate::alter::rewrite_add_columns_plural;
use crate::normalize::{has_angle_map_column_type, normalized_parse_dialect};

fn tokenize(sql: &str) -> Vec<Token> {
    Tokenizer::new(&DatabricksDialect {}, sql)
        .tokenize()
        .unwrap_or_else(|error| panic!("{sql:?} must tokenize: {error}"))
}

fn render(tokens: &[Token]) -> String {
    tokens.iter().map(ToString::to_string).collect::<String>()
}

fn rewritten_add_columns(sql: &str) -> String {
    render(&rewrite_add_columns_plural(&tokenize(sql)))
}

#[test]
fn add_columns_plural_splitter_tracks_angle_brackets_at_depth_zero() {
    let rendered = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (s2 STRUCT<p: INT, q: STRING>, m2 MAP<STRING, INT>, \
         a2 ARRAY<STRING>)",
    );
    assert!(
        !rendered.contains("COLUMNS"),
        "the plural keyword must be fully rewritten, got: {rendered}"
    );
    assert_eq!(
        rendered.matches("ADD COLUMN").count(),
        3,
        "each nested def must survive intact, got: {rendered}"
    );
    assert!(
        rendered.contains("s2 STRUCT<p: INT, q: STRING>")
            && rendered.contains("m2 MAP<STRING, INT>")
            && rendered.contains("a2 ARRAY<STRING>"),
        "no def may be split or reordered, got: {rendered}"
    );
}

#[test]
fn add_columns_plural_splitter_handles_paren_depth_one_and_shift_close() {
    let decimal = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (a STRUCT<x: DECIMAL(10, 2), y: ARRAY<INT>>, b INT)",
    );
    assert_eq!(
        decimal.matches("ADD COLUMN").count(),
        2,
        "angle depth at paren depth 1 must hold, got: {decimal}"
    );
    let nested = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (a ARRAY<STRUCT<x: INT, y: STRING>>, b INT)",
    );
    assert_eq!(
        nested.matches("ADD COLUMN").count(),
        2,
        "`>>` closes two angle levels, got: {nested}"
    );
    let triple = rewritten_add_columns(
        "ALTER TABLE ice.ns.t ADD COLUMNS (a STRUCT<m: MAP<STRING, ARRAY<INT>>>, b INT)",
    );
    assert_eq!(
        triple.matches("ADD COLUMN").count(),
        2,
        "`>>>` closes three angle levels, got: {triple}"
    );
}

#[test]
fn add_columns_plural_splitter_splits_after_top_level_gt() {
    let sql = "ALTER TABLE ice.ns.t ADD COLUMNS (a INT DEFAULT 1 > 0, b INT)";
    let rendered = rewritten_add_columns(sql);
    assert_eq!(
        rendered.matches("ADD COLUMN").count(),
        2,
        "a top-level `>` must not suppress the comma split, got: {rendered}"
    );
    let parsed = parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
    let Some((statement, _, _)) = parsed else {
        panic!("{sql:?} must parse once the comma splits the two column defs");
    };
    if let Statement::AlterTable(alter) = &statement {
        assert_eq!(alter.operations.len(), 2);
    } else {
        panic!("{sql:?} must parse as ALTER TABLE, got: {statement}");
    }
}

#[test]
fn angle_map_alter_parses_under_widened_dialect() {
    let sql = "ALTER TABLE ice.ns.t ADD COLUMN m MAP<STRUCT<a: INT>, STRING>";
    assert!(
        has_angle_map_column_type(&tokenize(sql)),
        "a MAP< ALTER must trip the angle-map predicate"
    );
    let parsed = parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
    let Some((statement, _, _)) = parsed else {
        panic!("{sql:?} must parse after the widening");
    };
    assert!(matches!(statement, Statement::AlterTable(_)));
    let plural = "ALTER TABLE ice.ns.t ADD COLUMNS (s2 STRUCT<p: INT, q: STRING>, m2 MAP<STRING, \
     INT>, a2 ARRAY<STRING>)";
    let parsed =
        parse_single_normalized(plural).unwrap_or_else(|error| panic!("{plural:?}: {error}"));
    let Some((statement, _, _)) = parsed else {
        panic!("{plural:?} must parse after the splitter fix");
    };
    if let Statement::AlterTable(alter) = &statement {
        assert_eq!(alter.operations.len(), 3);
    } else {
        panic!("{plural:?} must parse as ALTER TABLE, got: {statement}");
    }
}

#[test]
fn clustered_by_reaches_partitioning_before_parse() {
    let sql = "CREATE TABLE ice.ns.t (id BIGINT, data STRING) USING iceberg CLUSTERED BY (id) \
     INTO 4 BUCKETS";
    let parsed = parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
    let Some((statement, partitioning, _)) = parsed else {
        panic!("{sql:?} must parse with its partitioning extracted");
    };
    assert!(matches!(statement, Statement::CreateTable(_)));
    assert_eq!(
        partitioning,
        vec![PartitionedByElement::Transform {
            name: "bucket".to_string(),
            args: vec!["4".to_string(), "id".to_string()],
        }]
    );
}

#[test]
fn date_alias_names_the_field_ts_day() {
    for (alias, canonical) in [("date", "day"), ("date_hour", "hour")] {
        assert_eq!(
            build_transform_field(alias, &["ts".to_string()]).unwrap(),
            build_transform_field(canonical, &["ts".to_string()]).unwrap(),
            "{alias} must alias {canonical}"
        );
    }
    assert_eq!(
        build_transform_field("date", &["ts".to_string()]).unwrap(),
        PartitionFieldSpec::Day("ts".to_string())
    );
    assert_eq!(
        build_transform_field("date_hour", &["ts".to_string()]).unwrap(),
        PartitionFieldSpec::Hour("ts".to_string())
    );
}

#[test]
fn bucket_and_truncate_both_argument_orders_give_the_same_spec() {
    let build = |name: &str, first: &str, second: &str, label: &str| -> PartitionFieldSpec {
        build_transform_field(name, &[first.to_string(), second.to_string()])
            .unwrap_or_else(|error| panic!("{label} must build: {error}"))
    };
    let bucket_first = build("bucket", "4", "id", "bucket(4, id)");
    let bucket_second = build("bucket", "id", "4", "bucket(id, 4)");
    assert_eq!(bucket_first, bucket_second);
    assert_eq!(
        bucket_first,
        PartitionFieldSpec::Bucket {
            column: "id".to_string(),
            num_buckets: 4,
        }
    );
    let truncate_first = build("truncate", "4", "data", "truncate(4, data)");
    let truncate_second = build("truncate", "data", "4", "truncate(data, 4)");
    assert_eq!(truncate_first, truncate_second);
    assert_eq!(
        truncate_first,
        PartitionFieldSpec::Truncate {
            column: "data".to_string(),
            width: 4,
        }
    );
}

#[test]
fn bucket_two_integer_arguments_raises() {
    let error = build_transform_field("bucket", &["1".to_string(), "2".to_string()])
        .expect_err("bucket(1, 2) is ambiguous and must refuse");
    assert!(error.to_string().contains("bucket"), "got: {error}");
    let truncate_error = build_transform_field("truncate", &["1".to_string(), "2".to_string()])
        .expect_err("truncate(1, 2) is ambiguous and must refuse");
    assert!(
        truncate_error.to_string().contains("truncate"),
        "got: {truncate_error}"
    );
}

#[test]
fn bucket_two_non_integer_arguments_refuses() {
    let error = build_transform_field("bucket", &["a".to_string(), "b".to_string()])
        .expect_err("bucket(\"a\", \"b\") has no integer width and must refuse");
    assert!(error.to_string().contains("integer"), "got: {error}");
}

#[test]
fn render_transform_arg_keeps_string_literals_quoted() {
    let single =
        crate::normalize::render_transform_arg(&[&Token::SingleQuotedString("x".to_string())]);
    let double =
        crate::normalize::render_transform_arg(&[&Token::DoubleQuotedString("x".to_string())]);
    assert_eq!(single, "'x'");
    assert_eq!(double, "\"x\"");
}

#[test]
fn bucket_quoted_string_arguments_are_never_a_width() {
    for args in [
        vec!["'x'".to_string(), "16".to_string()],
        vec!["\"x\"".to_string(), "16".to_string()],
        vec!["'16'".to_string(), "id".to_string()],
        vec!["16".to_string(), "'x'".to_string()],
        vec!["16".to_string(), "\"x\"".to_string()],
    ] {
        let error = build_transform_field("bucket", &args)
            .expect_err("a quoted string is never a bucket width and must refuse");
        assert!(
            error.to_string().contains("numBuckets must be an integer"),
            "got: {error}"
        );
    }
}

#[test]
fn truncate_quoted_string_arguments_are_never_a_width() {
    for args in [
        vec!["'day'".to_string(), "4".to_string()],
        vec!["\"day\"".to_string(), "4".to_string()],
        vec!["4".to_string(), "'day'".to_string()],
        vec!["4".to_string(), "\"day\"".to_string()],
    ] {
        let error = build_transform_field("truncate", &args)
            .expect_err("a quoted string is never a truncate width and must refuse");
        assert!(
            error.to_string().contains("width must be an integer"),
            "got: {error}"
        );
    }
}

#[test]
fn replace_partition_field_transform_lhs_parses_to_by_transform_change() {
    let sql = "ALTER TABLE ice.ns.t REPLACE PARTITION FIELD days(ts) WITH hours(ts)";
    let parsed = crate::alter::try_parse_iceberg_alter_ddl(sql)
        .expect("REPLACE PARTITION FIELD transform LHS must claim the statement")
        .expect("statement must parse");
    let crate::alter::IcebergAlterDdl::PartitionSpec { changes, .. } = parsed else {
        panic!("{sql} must parse as a partition-spec change");
    };
    assert_eq!(changes.len(), 1);
    assert!(
        matches!(
            &changes[0],
            PartitionSpecChange::ReplaceFieldByTransform {
                old_source_name,
                old_transform: Transform::Day,
                source_name,
                transform: Transform::Hour,
                new_name: None,
            } if old_source_name == "ts" && source_name == "ts"
        ),
        "got: {:?}",
        changes[0]
    );
}

#[tokio::test]
async fn replace_partition_field_transform_lhs_resolves_and_refuses_no_match() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    execute(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.rpf (ts TIMESTAMP, id BIGINT) USING iceberg",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rpf ADD PARTITION FIELD days(ts)",
    )
    .await
    .unwrap();
    execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rpf REPLACE PARTITION FIELD days(ts) WITH hours(ts)",
    )
    .await
    .unwrap();
    let handle = catalog_handle(&catalogs, "ice").unwrap();
    let table = handle
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            "rpf".into(),
        ))
        .await
        .unwrap();
    let rendered: Vec<(String, String)> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.transform.to_string()))
        .collect();
    assert_eq!(rendered, vec![("ts_hour".to_string(), "hour".to_string())]);

    let no_match = execute(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.rpf REPLACE PARTITION FIELD bucket(4, id) WITH bucket(8, id)",
    )
    .await
    .expect_err("a transform LHS matching no field must refuse loud");
    assert!(
        no_match.to_string().contains("matches no partition field"),
        "got: {no_match}"
    );
}

#[test]
fn dialect_widening_changes_no_other_alter() {
    for (sql, parses) in [
        ("ALTER TABLE ice.ns.t SET TBLPROPERTIES ('k'='v')", true),
        ("ALTER TABLE ice.ns.t UNSET TBLPROPERTIES ('k')", true),
        ("ALTER TABLE ice.ns.t ADD COLUMN c1 INT", true),
        ("ALTER TABLE ice.ns.t ADD COLUMNS (c2 INT, c3 STRING)", true),
        ("ALTER TABLE ice.ns.t DROP COLUMN c3", true),
        ("ALTER TABLE ice.ns.t DROP COLUMNS (c2, c3)", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN c1 TYPE BIGINT", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN c1 DROP NOT NULL", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN c1 SET NOT NULL", true),
        ("ALTER TABLE ice.ns.t ALTER COLUMN b FIRST", false),
        ("ALTER TABLE ice.ns.t ALTER COLUMN b AFTER id", false),
        (
            "ALTER TABLE ice.ns.t ADD PARTITION FIELD bucket(8, id)",
            false,
        ),
        ("ALTER TABLE ice.ns.t DROP PARTITION FIELD id_b8", false),
        (
            "ALTER TABLE ice.ns.t REPLACE PARTITION FIELD id_b8 WITH bucket(16, id)",
            false,
        ),
    ] {
        assert!(
            !has_angle_map_column_type(&tokenize(sql)),
            "{sql:?} must stay off the angle-map predicate"
        );
        let parsed =
            parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        assert_eq!(
            parsed.is_some(),
            parses,
            "{sql:?} must parse exactly as before the widening"
        );
    }
    let select = "SELECT CAST(x AS MAP<STRING, INT>) FROM t";
    let parsed =
        parse_single_normalized(select).unwrap_or_else(|error| panic!("{select:?}: {error}"));
    assert!(
        parsed.is_none(),
        "a MAP<> outside CREATE/ALTER keeps its dialect and stays unparsed"
    );
    let plain = "SELECT id, name FROM t";
    let parsed =
        parse_single_normalized(plain).unwrap_or_else(|error| panic!("{plain:?}: {error}"));
    assert!(parsed.is_some(), "an ordinary SELECT still parses");
}

#[test]
fn normalized_parse_dialect_widens_only_angle_map_statements() {
    let alter_map = normalized_parse_dialect(&tokenize(
        "ALTER TABLE ice.ns.t ADD COLUMN m MAP<STRING, INT>",
    ));
    assert!(
        alter_map.is::<SparkSqlDialect>(),
        "an ALTER carrying MAP< must take SparkSqlDialect"
    );
    let alter_plain = normalized_parse_dialect(&tokenize("ALTER TABLE ice.ns.t ADD COLUMN c INT"));
    assert!(
        alter_plain.is::<GenericDialect>(),
        "an ALTER without MAP< must stay on GenericDialect"
    );
    let create_map =
        normalized_parse_dialect(&tokenize("CREATE TABLE ice.ns.t (m MAP<STRING, INT>)"));
    assert!(
        create_map.is::<SparkSqlDialect>(),
        "a CREATE carrying MAP< must take SparkSqlDialect"
    );
    let select = normalized_parse_dialect(&tokenize("SELECT id FROM t"));
    assert!(
        select.is::<DatabricksDialect>(),
        "any other statement keeps the Databricks dialect"
    );
}

#[test]
fn table_comment_extracts_on_either_side_of_tblproperties() {
    for sql in [
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg COMMENT 'tbl' TBLPROPERTIES ('k' = 'v')",
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k' = 'v') COMMENT 'tbl'",
        "CREATE TABLE ice.ns.t USING iceberg TBLPROPERTIES ('k' = 'v') COMMENT 'tbl' AS SELECT 1",
    ] {
        let parsed =
            parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        let Some((statement, _, clauses)) = parsed else {
            panic!("{sql:?} must parse with its comment extracted");
        };
        assert!(
            matches!(statement, Statement::CreateTable(_)),
            "{sql:?} must stay a CreateTable"
        );
        assert_eq!(
            clauses.comment.as_deref(),
            Some("tbl"),
            "{sql:?} keeps its comment"
        );
    }
}

#[test]
fn table_location_extracts_on_either_side_of_tblproperties() {
    for sql in [
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg LOCATION '/a' TBLPROPERTIES ('k' = 'v')",
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg TBLPROPERTIES ('k' = 'v') LOCATION '/a'",
    ] {
        let parsed =
            parse_single_normalized(sql).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        let Some((statement, _, clauses)) = parsed else {
            panic!("{sql:?} must parse with its location extracted");
        };
        assert!(
            matches!(statement, Statement::CreateTable(_)),
            "{sql:?} must stay a CreateTable"
        );
        assert_eq!(
            clauses.location.as_deref(),
            Some("/a"),
            "{sql:?} keeps its location"
        );
    }
}

#[test]
fn create_options_rewrite_stores_both_keys_before_parse() {
    use datafusion::sql::sqlparser::ast::{CreateTableOptions, SqlOption};

    for (sql, expected) in [
        (
            "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg OPTIONS ('k1'='v1')",
            vec![("k1", "v1"), ("option.k1", "v1")],
        ),
        (
            "CREATE TABLE ice.ns.t USING iceberg OPTIONS ('k'='v') AS SELECT 1 AS i",
            vec![("k", "v"), ("option.k", "v")],
        ),
    ] {
        let canonical = crate::spark_literals::canonicalize(sql)
            .unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        let parsed =
            parse_single_normalized(&canonical).unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        let Some((Statement::CreateTable(create), _, _)) = parsed else {
            panic!("{sql:?} must parse as CreateTable after the OPTIONS rewrite");
        };
        let CreateTableOptions::TableProperties(options) = &create.table_options else {
            panic!(
                "{sql:?} must carry TBLPROPERTIES after the rewrite, got {:?}",
                create.table_options
            );
        };
        let mut pairs: Vec<(String, String)> = Vec::new();
        for option in options {
            if let SqlOption::KeyValue { key, value } = option {
                pairs.push((key.value.clone(), property_value(value)));
            }
        }
        assert_eq!(
            pairs.len(),
            expected.len(),
            "{sql:?} must store raw and option.-prefixed keys, got {pairs:?}"
        );
        for ((got_key, got_value), (want_key, want_value)) in pairs.iter().zip(expected.iter()) {
            assert_eq!(got_key, want_key, "{sql:?} must store key {want_key}");
            assert_eq!(
                got_value, want_value,
                "{sql:?} must store value of {want_key}"
            );
        }
    }
}

#[test]
fn create_options_rewrite_stays_off_non_iceberg_and_mixed_shapes() {
    use datafusion::sql::sqlparser::ast::CreateTableOptions;

    for sql in [
        "CREATE TABLE ice.ns.t (id BIGINT) USING parquet OPTIONS ('k'='v')",
        "CREATE TABLE ice.ns.t (id BIGINT) OPTIONS ('k'='v')",
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg WITH ('k'='v')",
        "CREATE TABLE ice.ns.t (id BIGINT) USING iceberg OPTIONS ('k' 'v')",
    ] {
        let canonical = crate::spark_literals::canonicalize(sql)
            .unwrap_or_else(|error| panic!("{sql:?}: {error}"));
        let parsed = parse_single_normalized(&canonical);
        let carries_tblproperties = matches!(&parsed, Ok(Some((Statement::CreateTable(create), _, _)))
            if matches!(create.table_options, CreateTableOptions::TableProperties(_)));
        assert!(
            !carries_tblproperties,
            "{sql:?} must not gain TBLPROPERTIES from the OPTIONS rewrite, got {parsed:?}"
        );
    }
    let canonical = crate::spark_literals::canonicalize(
        "CREATE TABLE ice.ns.t (id BIGINT) USING parquet OPTIONS ('k'='v')",
    )
    .unwrap();
    assert!(
        !canonical.contains("TBLPROPERTIES"),
        "a non-iceberg provider must not be rewritten, got: {canonical}"
    );
}

#[test]
fn create_table_options_refusal_keeps_plain_and_with_drops_options() {
    use datafusion::sql::sqlparser::ast::CreateTable;
    use datafusion::sql::sqlparser::dialect::GenericDialect;

    let parse_create = |sql: &str| -> CreateTable {
        let statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse");
        match statements.into_iter().next().expect("one statement") {
            Statement::CreateTable(create) => create,
            other => panic!("expected CreateTable, got {other}"),
        }
    };
    let with = parse_create("CREATE TABLE cat.ns.t (id BIGINT) WITH ('k'='v')");
    let err = refuse_unsupported_create_table_clauses(&with, "column-def CREATE").unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("WITH/plain options are not supported")
            && message.contains("use TBLPROPERTIES for Iceberg table properties")
            && !message.contains("OPTIONS"),
        "got: {message}"
    );
    let plain = parse_create("CREATE TABLE cat.ns.t (id BIGINT) ENGINE = InnoDB");
    let err = refuse_unsupported_create_table_clauses(&plain, "CTAS").unwrap_err();
    assert!(
        err.to_string()
            .contains("WITH/plain options are not supported"),
        "got: {err}"
    );
    let options = parse_create("CREATE TABLE cat.ns.t (id BIGINT) OPTIONS ('k'='v')");
    assert!(
        refuse_unsupported_create_table_clauses(&options, "column-def CREATE").is_ok(),
        "the OPTIONS refusal is deleted: the variant must not refuse"
    );
}

fn metadata_json_files(location: &std::path::Path) -> Vec<std::path::PathBuf> {
    let dir = location.join("metadata");
    let mut files: Vec<std::path::PathBuf> = if dir.is_dir() {
        std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.to_string_lossy().ends_with(".metadata.json"))
            .collect()
    } else {
        Vec::new()
    };
    files.sort();
    files
}

fn parquet_files(location: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    if location.is_dir() {
        for entry in std::fs::read_dir(location)
            .unwrap()
            .filter_map(std::result::Result::ok)
        {
            let path = entry.path();
            if path.is_dir() {
                files.extend(parquet_files(&path));
            } else if path.to_string_lossy().ends_with(".parquet") {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

#[tokio::test]
async fn set_location_moves_the_metadata_location_and_the_next_commit_lands_under_it() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.loc (id BIGINT) USING iceberg",
    )
    .await;
    let old_location = wh.path().join("sales").join("loc");
    let new_location = wh.path().join("moved").join("loc");
    run(
        &ctx,
        &catalogs,
        &format!(
            "ALTER TABLE ice.sales.loc SET LOCATION '{}'",
            new_location.display()
        ),
    )
    .await;
    run(&ctx, &catalogs, "INSERT INTO ice.sales.loc VALUES (1)").await;

    let handle = catalog_handle(&catalogs, "ice").unwrap();
    let table = handle
        .load_table(&TableIdent::new(
            NamespaceIdent::new("sales".into()),
            "loc".into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        table.metadata().location(),
        new_location.to_str().unwrap(),
        "the metadata `location` field must record the new path"
    );
    assert_eq!(
        metadata_json_files(&old_location).len(),
        1,
        "the original metadata file is never moved"
    );
    assert_eq!(
        metadata_json_files(&new_location).len(),
        2,
        "the move commit and the next metadata commit (the INSERT) both land under the new \
         location"
    );
    assert_eq!(
        parquet_files(&new_location).len(),
        1,
        "the post-move INSERT writes its data file under the new location"
    );
    assert_eq!(
        rows(&ctx, &catalogs, "SELECT id FROM ice.sales.loc").await,
        1
    );
}

#[tokio::test]
async fn set_location_near_misses_keep_their_own_routing() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(&ctx, &catalogs, "CREATE NAMESPACE ice.drop").await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.drop.tag (id BIGINT) USING iceberg",
    )
    .await;
    let moved = wh.path().join("moved").join("tag");
    run(
        &ctx,
        &catalogs,
        &format!(
            "ALTER TABLE ice.drop.tag SET LOCATION '{}'",
            moved.display()
        ),
    )
    .await;
    let handle = catalog_handle(&catalogs, "ice").unwrap();
    let table = handle
        .load_table(&TableIdent::new(
            NamespaceIdent::new("drop".into()),
            "tag".into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        table.metadata().location(),
        moved.to_str().unwrap(),
        "table-name segments that look like DDL verbs must not route to branch/tag DDL"
    );
}

#[tokio::test]
async fn set_location_refuses_write_options_and_missing_literal_loud() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = setup(&wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.sl (id BIGINT) USING iceberg",
    )
    .await;

    let options = crate::write_options::StatementWriteOptions::validate(vec![(
        "compression-codec".to_string(),
        "zstd".to_string(),
    )])
    .unwrap();
    let options_error = crate::execute_with_statement_options(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.sl SET LOCATION '/tmp/x'",
        &std::collections::HashSet::<String>::new(),
        &options,
    )
    .await
    .expect_err("SET LOCATION must go through the ALTER TABLE write-options gate");
    assert!(
        options_error
            .to_string()
            .contains("ALTER TABLE does not support write options"),
        "got: {options_error}"
    );

    let missing = execute(&ctx, &catalogs, "ALTER TABLE ice.sales.sl SET LOCATION")
        .await
        .expect_err("a missing path must refuse loud, never silently drop");
    assert!(
        missing.to_string().contains("SET LOCATION"),
        "got: {missing}"
    );
}
