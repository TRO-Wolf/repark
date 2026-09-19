use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, Ident, Value, ValueWithSpan};
use iceberg::spec::{
    NestedField, NestedFieldRef, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableCreation};
use tempfile::TempDir;

use crate::write::{
    OverwriteIntent, OverwriteMode, OverwritePlan, OverwriteScope,
    overwrite_mode_option_is_dynamic, partition_overwrite_request_from_exprs, plan_overwrite,
    replace_partitions_is_noop,
};

fn mode(session_dynamic: bool, intent: OverwriteIntent, option_dynamic: bool) -> OverwriteMode {
    OverwriteMode {
        session_dynamic,
        intent,
        option_dynamic,
    }
}

fn name(text: &str) -> Expr {
    Expr::Identifier(Ident::new(text))
}

fn assign(column: &str, text: &str) -> Expr {
    Expr::BinaryOp {
        left: Box::new(name(column)),
        op: BinaryOperator::Eq,
        right: Box::new(Expr::Value(ValueWithSpan::from(Value::SingleQuotedString(
            text.to_string(),
        )))),
    }
}

async fn table_with(warehouse: &TempDir, spec: Option<UnboundPartitionSpec>) -> Table {
    let fields = vec![
        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(3, "cat", Type::Primitive(PrimitiveType::String)).into(),
        NestedField::optional(4, "sub", Type::Primitive(PrimitiveType::String)).into(),
    ];
    table_of(warehouse, fields, spec).await
}

async fn table_of(
    warehouse: &TempDir,
    fields: Vec<NestedFieldRef>,
    spec: Option<UnboundPartitionSpec>,
) -> Table {
    let catalog = crate::catalog::memory_catalog(warehouse.path().to_str().expect("utf8"))
        .await
        .expect("catalog");
    let namespace = NamespaceIdent::new("ns".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema);
    let creation = match spec {
        Some(spec) => creation.partition_spec(spec).build(),
        None => creation.build(),
    };
    catalog
        .create_table(&namespace, creation)
        .await
        .expect("create")
}

fn by_cat_sub() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder()
        .add_partition_field(3, "cat", Transform::Identity)
        .expect("cat")
        .add_partition_field(4, "sub", Transform::Identity)
        .expect("sub")
        .build()
}

#[test]
fn decision_table_matches_spark_and_iceberg() {
    use OverwriteIntent::{Dynamic, Session, Static};
    use OverwriteScope::{ReplacePartitions, RowFilter, WholeTable};
    let rows = [
        (false, Session, false, false, WholeTable),
        (false, Session, false, true, RowFilter),
        (false, Session, true, false, ReplacePartitions),
        (false, Session, true, true, RowFilter),
        (true, Session, false, false, ReplacePartitions),
        (true, Session, false, true, ReplacePartitions),
        (true, Session, true, true, ReplacePartitions),
        (true, Static, false, false, WholeTable),
        (true, Static, true, false, WholeTable),
        (false, Static, true, false, WholeTable),
        (false, Dynamic, false, false, ReplacePartitions),
        (false, Dynamic, false, true, ReplacePartitions),
        (true, Dynamic, false, false, ReplacePartitions),
    ];
    for (session_dynamic, intent, option_dynamic, has_static, want) in rows {
        assert_eq!(
            mode(session_dynamic, intent, option_dynamic).scope(has_static),
            want,
            "session_dynamic={session_dynamic} intent={intent:?} option={option_dynamic} \
             static={has_static}"
        );
    }
}

#[test]
fn overwrite_mode_option_reads_dynamic_in_any_case_only() {
    for raw in ["dynamic", "DYNAMIC", "DyNaMiC"] {
        assert!(overwrite_mode_option_is_dynamic(raw), "{raw}");
    }
    for raw in ["static", "STATIC", "bogus", "", "dynamic "] {
        assert!(!overwrite_mode_option_is_dynamic(raw), "{raw}");
    }
}

#[tokio::test]
async fn plan_scopes_mixed_and_dynamic_clauses_by_mode() {
    let warehouse = TempDir::new().expect("warehouse");
    let table = table_with(&warehouse, Some(by_cat_sub())).await;
    let mixed =
        partition_overwrite_request_from_exprs(&[assign("cat", "x"), name("sub")]).expect("mixed");
    let static_mode = mode(false, OverwriteIntent::Session, false);
    let dynamic_mode = mode(true, OverwriteIntent::Session, false);
    match plan_overwrite(&table, &mixed, static_mode).expect("static plan") {
        OverwritePlan::RowFilter(spec) => {
            assert_eq!(spec.equalities, mixed.equalities);
            assert_eq!(spec.predicate.to_string(), "cat = \"x\"");
        }
        other => panic!("static mixed must filter by the static values, got {other:?}"),
    }
    match plan_overwrite(&table, &mixed, dynamic_mode).expect("dynamic plan") {
        OverwritePlan::ReplacePartitions(equalities) => assert_eq!(equalities, mixed.equalities),
        other => panic!("dynamic mixed must replace partitions, got {other:?}"),
    }
    let both = partition_overwrite_request_from_exprs(&[name("cat"), name("sub")]).expect("both");
    assert!(matches!(
        plan_overwrite(&table, &both, static_mode).expect("static"),
        OverwritePlan::WholeTable
    ));
    assert!(matches!(
        plan_overwrite(&table, &both, dynamic_mode).expect("dynamic"),
        OverwritePlan::ReplacePartitions(equalities) if equalities.is_empty()
    ));
}

#[tokio::test]
async fn plan_refuses_a_non_partition_column_like_spark() {
    let warehouse = TempDir::new().expect("warehouse");
    let unpartitioned = table_with(&warehouse, None).await;
    for exprs in [vec![name("cat")], vec![assign("cat", "x")]] {
        let request = partition_overwrite_request_from_exprs(&exprs).expect("request");
        for session_dynamic in [false, true] {
            let error = plan_overwrite(
                &unpartitioned,
                &request,
                mode(session_dynamic, OverwriteIntent::Session, false),
            )
            .expect_err("unpartitioned PARTITION (cat) must refuse");
            assert_eq!(
                error.strip_backtrace(),
                "Error during planning: [NON_PARTITION_COLUMN] PARTITION clause cannot contain \
                 the non-partition column: `cat`. SQLSTATE: 42000"
            );
        }
    }
    let empty = partition_overwrite_request_from_exprs(&[]).expect("empty");
    assert!(matches!(
        plan_overwrite(&unpartitioned, &empty, OverwriteMode::default()).expect("empty clause"),
        OverwritePlan::WholeTable
    ));
    let other = TempDir::new().expect("warehouse");
    let partitioned = table_with(&other, Some(by_cat_sub())).await;
    let request =
        partition_overwrite_request_from_exprs(&[assign("cat", "x"), name("data")]).expect("data");
    let error = plan_overwrite(&partitioned, &request, OverwriteMode::default())
        .expect_err("data is not a partition column");
    assert_eq!(
        error.strip_backtrace(),
        "Error during planning: [NON_PARTITION_COLUMN] PARTITION clause cannot contain the \
         non-partition column: `data`. SQLSTATE: 42000"
    );
}

fn assign_expr(column: &str, value: Expr) -> Expr {
    Expr::BinaryOp {
        left: Box::new(name(column)),
        op: BinaryOperator::Eq,
        right: Box::new(value),
    }
}

fn timestamp_literal(text: &str) -> Expr {
    Expr::TypedString(datafusion::sql::sqlparser::ast::TypedString {
        data_type: datafusion::sql::sqlparser::ast::DataType::Timestamp(
            None,
            datafusion::sql::sqlparser::ast::TimezoneInfo::None,
        ),
        value: ValueWithSpan::from(Value::SingleQuotedString(text.to_string())),
        uses_odbc_syntax: false,
    })
}

fn non_partition_column(column: &str) -> String {
    format!(
        "Error during planning: [NON_PARTITION_COLUMN] PARTITION clause cannot contain the \
         non-partition column: `{column}`. SQLSTATE: 42000"
    )
}

#[tokio::test]
async fn plan_refuses_a_transform_source_key_before_reading_its_value() {
    let warehouse = TempDir::new().expect("warehouse");
    let bucket = UnboundPartitionSpec::builder()
        .add_partition_field(3, "cat", Transform::Identity)
        .expect("cat")
        .add_partition_field(1, "id_bucket", Transform::Bucket(4))
        .expect("bucket")
        .build();
    let table = table_with(&warehouse, Some(bucket)).await;
    let one = Expr::Value(ValueWithSpan::from(Value::Number("1".to_string(), false)));
    let cases = [
        (vec![assign_expr("id", one)], "id"),
        (vec![name("id")], "id"),
        (vec![name("id_bucket")], "id_bucket"),
        (
            vec![assign_expr("id", timestamp_literal("2024-01-01"))],
            "id",
        ),
    ];
    for (exprs, column) in cases {
        let request = partition_overwrite_request_from_exprs(&exprs).expect("request");
        for session_dynamic in [false, true] {
            let error = plan_overwrite(
                &table,
                &request,
                mode(session_dynamic, OverwriteIntent::Session, false),
            )
            .expect_err("a transform source is not a partition column");
            assert_eq!(error.strip_backtrace(), non_partition_column(column));
        }
    }
    let typed = partition_overwrite_request_from_exprs(&[assign_expr(
        "cat",
        timestamp_literal("2024-01-01"),
    )])
    .expect("request");
    let error = plan_overwrite(&table, &typed, OverwriteMode::default())
        .expect_err("a typed literal on an identity key keeps its refusal");
    assert!(
        error
            .to_string()
            .contains("INSERT OVERWRITE PARTITION assignment value must be a literal"),
        "{error}"
    );
}

#[tokio::test]
async fn plan_casts_a_static_value_to_the_partition_source_type() {
    let warehouse = TempDir::new().expect("warehouse");
    let fields = vec![
        NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
        NestedField::optional(2, "d", Type::Primitive(PrimitiveType::Date)).into(),
    ];
    let spec = UnboundPartitionSpec::builder()
        .add_partition_field(2, "d", Transform::Identity)
        .expect("d")
        .build();
    let table = table_of(&warehouse, fields, Some(spec)).await;
    let valid = partition_overwrite_request_from_exprs(&[assign("d", "2024-01-01")]).expect("ok");
    match plan_overwrite(&table, &valid, OverwriteMode::default()).expect("cast plan") {
        OverwritePlan::RowFilter(spec) => {
            assert_eq!(spec.predicate.to_string(), "d = 2024-01-01");
        }
        other => panic!("a static DATE value must filter its partition, got {other:?}"),
    }
    let invalid =
        partition_overwrite_request_from_exprs(&[assign("d", "2024-13-45")]).expect("request");
    let error = plan_overwrite(&table, &invalid, OverwriteMode::default())
        .expect_err("the cast rejects the value");
    assert_eq!(
        error.strip_backtrace(),
        "Arrow error: Cast error: Cannot cast string '2024-13-45' to value of Date32 type"
    );
}

#[test]
fn an_empty_dynamic_stage_skips_the_commit() {
    assert!(replace_partitions_is_noop(&[]));
}
