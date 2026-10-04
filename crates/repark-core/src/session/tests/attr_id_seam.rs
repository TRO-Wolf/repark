use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::dataframe::DataFrame;
use datafusion::functions::core::expr_ext::FieldAccessor;
use datafusion::functions_aggregate::expr_fn::{count, max, sum};
use datafusion::functions_window::expr_fn::{lag, row_number};
use datafusion::logical_expr::{LogicalPlan, cast, col, lit, try_cast, when};

use crate::ReparkSession;
use crate::frame_names::{AttrId, stamp, strip_for_execution};

const KEY: &str = "repark.attr";

fn batch(schema: Arc<Schema>) -> RecordBatch {
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec!["1", "2", "x"])),
        Arc::new(Int64Array::from(vec![10, 20, 30])),
    ];
    RecordBatch::try_new(schema, columns).unwrap()
}

fn clean_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]))
}

fn foreign_schema() -> Arc<Schema> {
    let tagged = |name: &str, data_type: DataType, id: &str| {
        Field::new(name, data_type, true)
            .with_metadata(HashMap::from([(KEY.to_string(), id.to_string())]))
    };
    Arc::new(Schema::new(vec![
        tagged("id", DataType::Utf8, "a0000000f0001"),
        tagged("v", DataType::Int64, "a0000000f0002"),
    ]))
}

fn stamped(frame: DataFrame) -> DataFrame {
    let (state, plan) = frame.into_parts();
    DataFrame::new(state, stamp(plan).unwrap())
}

fn clean_source(session: &ReparkSession) -> DataFrame {
    stamped(session.context().read_batch(batch(clean_schema())).unwrap())
}

fn failed_casts(frame: DataFrame) -> DataFrame {
    let failed = col("id")
        .is_not_null()
        .and(try_cast(col("id"), DataType::Int64).is_null());
    let value = when(failed, lit(1)).otherwise(lit(0)).unwrap();
    stamped(
        frame
            .aggregate(vec![], vec![sum(value).alias("failed")])
            .unwrap(),
    )
}

fn plan_carries_id(plan: &LogicalPlan) -> bool {
    let mut keyed = false;
    plan.apply_with_subqueries(|node| {
        keyed = node
            .schema()
            .fields()
            .iter()
            .any(|field| AttrId::of(field).is_some());
        Ok(if keyed {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .unwrap();
    keyed
}

async fn optimized_is_clean_and_runs(frame: &DataFrame) -> usize {
    let rows = frame
        .clone()
        .collect()
        .await
        .unwrap()
        .iter()
        .map(RecordBatch::num_rows)
        .sum();
    let optimized = frame.clone().into_optimized_plan().unwrap();
    assert!(!plan_carries_id(&optimized), "{optimized}");
    rows
}

#[test]
fn every_session_strips_ids_first_in_its_optimizer_and_never_in_its_analyzer() {
    let session = ReparkSession::new().unwrap();
    let state = session.context().state();
    let optimizer = state
        .optimizer()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect::<Vec<_>>();
    assert_eq!(optimizer[0], "repark_strip_attribute_ids");
    assert!(
        state
            .analyzer()
            .rules
            .iter()
            .all(|rule| rule.name() != "repark_strip_attribute_ids")
    );
}

#[tokio::test]
async fn an_analyzed_plan_keeps_its_ids_for_the_eager_sql_door() {
    let session = ReparkSession::new().unwrap();
    let source = clean_source(&session);
    let state = session.context().state();
    let analyzed = state
        .analyzer()
        .execute_and_check(
            failed_casts(source).logical_plan().clone(),
            state.config_options(),
            |_, _| {},
        )
        .unwrap();
    assert!(plan_carries_id(&analyzed));
}

#[tokio::test]
async fn an_aggregate_over_a_stamped_source_plans_and_runs() {
    let session = ReparkSession::new().unwrap();
    let source = clean_source(&session);
    assert!(
        source
            .schema()
            .fields()
            .iter()
            .all(|field| AttrId::of(field).is_some())
    );
    let aggregated = failed_casts(source.clone());
    assert!(plan_carries_id(aggregated.logical_plan()));
    assert_eq!(optimized_is_clean_and_runs(&aggregated).await, 1);
    let rows = aggregated.collect().await.unwrap();
    let failed = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(failed, 1);
    assert!(
        source
            .schema()
            .fields()
            .iter()
            .all(|field| AttrId::of(field).is_some())
    );
}

#[tokio::test]
async fn aggregates_over_stamped_windows_unions_joins_and_subqueries_run() {
    let session = ReparkSession::new().unwrap();
    let source = clean_source(&session);
    let windowed = stamped(
        source
            .clone()
            .window(vec![
                row_number().alias("n"),
                lag(col("v"), Some(1), None).alias("prev"),
            ])
            .unwrap(),
    );
    let grouped = stamped(
        windowed
            .aggregate(vec![col("id")], vec![max(col("prev")), count(col("n"))])
            .unwrap(),
    );
    assert_eq!(optimized_is_clean_and_runs(&grouped).await, 3);
    let casted = stamped(
        source
            .clone()
            .select(vec![cast(col("v"), DataType::Utf8).alias("id"), col("v")])
            .unwrap(),
    );
    let unioned = stamped(source.clone().union(casted).unwrap());
    assert_eq!(
        optimized_is_clean_and_runs(&failed_casts(unioned.clone())).await,
        1
    );
    let by_name = stamped(source.clone().union_by_name(source.clone()).unwrap());
    assert_eq!(optimized_is_clean_and_runs(&failed_casts(by_name)).await, 1);
    session
        .create_or_replace_temp_view_from("seam_l", &source)
        .unwrap();
    session
        .create_or_replace_temp_view_from("seam_u", &unioned)
        .unwrap();
    for sql in [
        "SELECT id, count(*) AS c FROM seam_l LEFT SEMI JOIN seam_u ON seam_l.id = seam_u.id \
         GROUP BY id",
        "SELECT a.id, sum(b.v) AS s FROM seam_l a JOIN seam_l b ON a.id = b.id GROUP BY a.id",
        "SELECT id, max(v) AS m FROM seam_l WHERE v > (SELECT min(v) FROM seam_u) GROUP BY id",
        "SELECT id, count(*) AS c FROM seam_l WHERE EXISTS \
         (SELECT 1 FROM seam_u WHERE seam_u.v = seam_l.v) GROUP BY id",
    ] {
        let frame = stamped(session.sql(sql).await.unwrap());
        assert!(plan_carries_id(frame.logical_plan()), "{sql}");
        let rows = optimized_is_clean_and_runs(&stamped(
            frame.aggregate(vec![], vec![count(lit(1))]).unwrap(),
        ))
        .await;
        assert_eq!(rows, 1, "{sql}");
    }
}

#[tokio::test]
async fn a_source_keyed_outside_the_session_keeps_datafusion_s_own_agreement() {
    let session = ReparkSession::new().unwrap();
    let foreign = session
        .context()
        .read_batch(batch(foreign_schema()))
        .unwrap();
    let fresh = FieldMetadata::from(HashMap::from([(
        KEY.to_string(),
        "a0000000f0009".to_string(),
    )]));
    let relabelled = foreign
        .select(vec![
            col("id").alias_with_metadata("id", Some(fresh)),
            col("v"),
        ])
        .unwrap();
    let aggregated = failed_casts(relabelled);
    let rows = aggregated.collect().await.unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

#[test]
fn a_stamped_frame_optimizes_to_the_plan_of_its_unstamped_twin() {
    let session = ReparkSession::new().unwrap();
    let struct_fields = vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("s", DataType::Struct(struct_fields.clone().into()), true),
    ]));
    let nested = StructArray::new(
        struct_fields.into(),
        vec![
            Arc::new(Int64Array::from(vec![5])),
            Arc::new(Int64Array::from(vec![6])),
        ],
        None,
    );
    let rows = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(nested)],
    )
    .unwrap();
    let raw = session.context().read_batch(rows).unwrap();
    let plain = raw.clone().select(vec![col("s").field("a")]).unwrap();
    let stamped_twin = stamped(stamped(raw).select(vec![col("s").field("a")]).unwrap());
    let optimized = |frame: DataFrame| {
        frame
            .into_optimized_plan()
            .unwrap()
            .display_indent()
            .to_string()
    };
    assert_eq!(optimized(stamped_twin), optimized(plain));
    let source = session.context().read_batch(batch(clean_schema())).unwrap();
    let grouped = |frame: DataFrame| {
        frame
            .aggregate(vec![col("id")], vec![max(col("v")).alias("m")])
            .unwrap()
    };
    assert_eq!(
        optimized(stamped(grouped(clean_source(&session)))),
        optimized(grouped(source))
    );
}

#[test]
fn a_stamped_plan_reaches_the_optimizer_backstop_and_its_collapsed_twin_plans_the_same() {
    let session = ReparkSession::new().unwrap();
    let frames = [
        clean_source(&session),
        failed_casts(clean_source(&session)),
        stamped(
            clean_source(&session)
                .aggregate(vec![col("id")], vec![max(col("v")).alias("m")])
                .unwrap(),
        ),
    ];
    for frame in frames {
        assert!(plan_carries_id(frame.logical_plan()));
        let backstop = frame.clone().into_optimized_plan().unwrap();
        assert!(!plan_carries_id(&backstop), "{backstop}");
        let (state, plan) = frame.into_parts();
        let twin = strip_for_execution(plan).unwrap();
        assert!(!plan_carries_id(&twin), "{twin}");
        let collapsed = DataFrame::new(state, twin).into_optimized_plan().unwrap();
        assert_eq!(
            collapsed.display_indent_schema().to_string(),
            backstop.display_indent_schema().to_string()
        );
    }
}
