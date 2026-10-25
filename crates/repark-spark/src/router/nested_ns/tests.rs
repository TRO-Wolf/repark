use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::common::{Column, DFSchema, ScalarValue};
use datafusion::logical_expr::{
    Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder, Projection, cast, lit,
};

use repark_functions::session_time_zone::SessionTimeZoneConfig;
use repark_iceberg::write::nested_ns_gate::{holds_naive_timestamp, holds_zoned_timestamp};

use super::{lineage_has, names_utc};

fn casts_a_zoned_value_to_naive(expr: &Expr, schema: &DFSchema) -> bool {
    let (input, target) = match expr {
        Expr::Cast(cast) => (cast.expr.as_ref(), cast.field.data_type()),
        Expr::TryCast(cast) => (cast.expr.as_ref(), cast.field.data_type()),
        _ => return false,
    };
    holds_naive_timestamp(target)
        && input
            .get_type(schema)
            .map_or(true, |source| holds_zoned_timestamp(&source))
}

fn instant_cast() -> LogicalPlan {
    let instant = lit(ScalarValue::TimestampMicrosecond(
        Some(0),
        Some("UTC".into()),
    ));
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    LogicalPlanBuilder::empty(true)
        .project(vec![cast(instant, naive).alias("a")])
        .and_then(LogicalPlanBuilder::build)
        .expect("plan")
}

#[test]
fn a_column_that_resolves_in_no_input_reads_every_input_whole() {
    let fields = vec![Field::new("b", DataType::Int32, true)];
    let schema = DFSchema::from_unqualified_fields(fields.into(), HashMap::new()).expect("schema");
    let outer = Projection::try_new_with_schema(
        vec![Expr::Column(Column::from_name("b"))],
        Arc::new(instant_cast()),
        Arc::new(schema),
    )
    .expect("projection");
    let outer = LogicalPlan::Projection(outer);
    assert!(lineage_has(&outer, &[0], casts_a_zoned_value_to_naive));
    assert!(lineage_has(
        &instant_cast(),
        &[0],
        casts_a_zoned_value_to_naive
    ));
}

#[test]
fn an_outer_reference_no_enclosing_join_resolves_fails_closed() {
    let field = Arc::new(Field::new("c", DataType::Int32, true));
    let outer = Expr::OuterReferenceColumn(field, Column::from_name("c"));
    let plan = LogicalPlanBuilder::empty(true)
        .project(vec![outer.alias("s")])
        .and_then(LogicalPlanBuilder::build)
        .expect("plan");
    assert!(lineage_has(&plan, &[0], casts_a_zoned_value_to_naive));
}

#[test]
fn an_unmodelled_node_that_refers_out_reads_the_other_side_whole() {
    let naive = Arc::new(Field::new(
        "a",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    ));
    let outer = Expr::OuterReferenceColumn(naive, Column::from_name("a"));
    let lateral = LogicalPlanBuilder::empty(true)
        .distinct_on(vec![lit(1)], vec![outer.alias("s")], None)
        .and_then(LogicalPlanBuilder::build)
        .expect("lateral");
    let plan = LogicalPlanBuilder::from(instant_cast())
        .cross_join(lateral)
        .and_then(LogicalPlanBuilder::build)
        .expect("join");
    assert!(lineage_has(&plan, &[1], casts_a_zoned_value_to_naive));
}

#[test]
fn only_a_pure_utc_alias_counts_as_utc() {
    for alias in [
        "UTC",
        "UCT",
        "Universal",
        "Zulu",
        "Z",
        "GMT",
        "GMT0",
        "GMT+0",
        "GMT-0",
        "Greenwich",
        "Etc/UTC",
        "Etc/UCT",
        "Etc/Universal",
        "Etc/Zulu",
        "Etc/GMT",
        "Etc/GMT0",
        "Etc/GMT+0",
        "Etc/GMT-0",
        "Etc/Greenwich",
        "+00:00",
        "-00:00",
    ] {
        let mut carrier = SessionTimeZoneConfig::default();
        carrier.set_zone(alias);
        assert!(names_utc(alias), "{alias}");
        assert!(names_utc(carrier.zone()), "{alias} as {}", carrier.zone());
    }
    for zone in [
        "Africa/Abidjan",
        "Atlantic/Reykjavik",
        "Europe/London",
        "America/New_York",
        "+01:00",
        "Etc/GMT+1",
    ] {
        let mut carrier = SessionTimeZoneConfig::default();
        carrier.set_zone(zone);
        assert!(!names_utc(zone), "{zone}");
        assert!(!names_utc(carrier.zone()), "{zone} as {}", carrier.zone());
    }
}
