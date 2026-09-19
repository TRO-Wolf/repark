use std::collections::HashMap;

use datafusion::sql::sqlparser::ast::{BinaryOperator, Expr, Ident, Value, ValueWithSpan};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type, UnboundPartitionSpec};
use iceberg::table::Table;
use iceberg::{NamespaceIdent, TableCreation};
use tempfile::TempDir;

use crate::write::{
    OverwriteIntent, OverwriteMode, OverwritePlan, OverwriteScope,
    overwrite_mode_option_is_dynamic, partition_overwrite_request_from_exprs, plan_overwrite,
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
        .with_fields(vec![
            NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "cat", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(4, "sub", Type::Primitive(PrimitiveType::String)).into(),
        ])
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
