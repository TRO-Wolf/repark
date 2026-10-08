use std::collections::BTreeMap;

use datafusion::functions_aggregate::expr_fn::count;
use datafusion::logical_expr::{JoinType, ident, lit};
use datafusion::prelude::{DataFrame, col};

use super::*;
use crate::microbatch::driver::{SinkSpec, StreamSpec, Trigger};
use crate::microbatch::testing::{Fixture, SINK, SOURCE, options, started};

fn caps() -> BTreeMap<String, String> {
    BTreeMap::from([(
        "streaming-max-files-per-micro-batch".to_string(),
        "1".to_string(),
    )])
}

async fn stream(fixture: &Fixture) -> DataFrame {
    streaming_frame(&fixture.session, SOURCE, &caps())
        .await
        .expect("a streaming frame")
}

fn refused(frame: &DataFrame) -> String {
    match PlanTemplate::from_frame(frame) {
        Err(MicroBatchError::StatefulOperatorRefused { operator }) => operator,
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn a_batch_action_on_a_streaming_frame_refuses() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1)").await;
    let frame = stream(&fixture).await;
    let error = frame.collect().await.expect_err("a streaming scan refuses");
    assert!(
        error.to_string().contains(STREAMING_SCAN_REFUSAL),
        "{error}"
    );
    let refusal = streaming_frame(
        &fixture.session,
        SOURCE,
        &BTreeMap::from([(
            "streaming-skip-delete-snapshots".to_string(),
            "true".to_string(),
        )]),
    )
    .await
    .expect_err("a refused option refuses at load");
    assert!(matches!(refusal, MicroBatchError::SkipOptionRefused { .. }));
}

#[tokio::test]
async fn stateful_operators_on_the_stream_refuse() {
    let fixture = Fixture::new().await;
    let frame = stream(&fixture).await;
    let aggregated = frame
        .clone()
        .aggregate(vec![], vec![count(col("id"))])
        .expect("aggregate");
    assert_eq!(refused(&aggregated), "aggregation");
    let distinct = frame.clone().distinct().expect("distinct");
    assert_eq!(refused(&distinct), "dropDuplicates");
    let sorted = frame
        .clone()
        .sort(vec![col("id").sort(true, false)])
        .expect("sort");
    assert_eq!(refused(&sorted), "sort");
    let limited = frame.clone().limit(0, Some(1)).expect("limit");
    assert_eq!(refused(&limited), "limit");
    let joined = frame
        .clone()
        .alias("l")
        .expect("alias")
        .join(
            frame.clone().alias("r").expect("alias"),
            JoinType::Inner,
            &["id"],
            &["id"],
            None,
        )
        .expect("join");
    assert_eq!(refused(&joined), "stream-stream join");
    let error = PlanTemplate::from_frame(&aggregated).expect_err("refused");
    assert_eq!(
        error.to_string(),
        "aggregation is not supported on a streaming DataFrame; use foreachBatch"
    );
}

#[tokio::test]
async fn stateless_shapes_and_a_static_side_are_accepted() {
    let fixture = Fixture::new().await;
    fixture.insert("ice.sales.other", "(2), (3)").await;
    let frame = stream(&fixture).await;
    let projected = frame
        .clone()
        .filter(col("id").gt(lit(1_i64)))
        .expect("filter")
        .select(vec![(col("id") * lit(10_i64)).alias("id")])
        .expect("select");
    let template = PlanTemplate::from_frame(&projected).expect("stateless");
    assert_eq!(template.source(), SOURCE);
    assert_eq!(template.source_options(), &caps());
    let explain = template.explain();
    assert!(
        explain.starts_with(
            "StreamingRelation iceberg ice.sales.orders [streaming-max-files-per-micro-batch=1]"
        ),
        "{explain}"
    );
    assert!(explain.contains("Filter"), "{explain}");
    let lookup = fixture
        .session
        .sql("SELECT id AS key, count(*) AS n FROM ice.sales.other GROUP BY id")
        .await
        .expect("a static aggregate");
    let enriched = frame
        .join(lookup, JoinType::Inner, &["id"], &["key"], None)
        .expect("join");
    PlanTemplate::from_frame(&enriched).expect("a stream-static join with a static aggregate");
}

#[tokio::test]
async fn a_batch_frame_is_not_a_template() {
    let fixture = Fixture::new().await;
    let frame = fixture
        .session
        .sql(&format!("SELECT id FROM {SOURCE}"))
        .await
        .expect("a batch frame");
    let error = PlanTemplate::from_frame(&frame).expect_err("no streaming source");
    assert!(
        matches!(&error, MicroBatchError::Catalog(message) if message.contains("streaming DataFrame")),
        "{error:?}"
    );
}

#[test]
fn only_append_output_mode_runs() {
    assert_eq!(check_output_mode("append"), Ok(()));
    assert_eq!(check_output_mode("Append"), Ok(()));
    for mode in ["complete", "update", "Complete"] {
        let error = check_output_mode(mode).expect_err("refused");
        assert_eq!(
            error,
            MicroBatchError::OutputModeRefused {
                mode: mode.to_ascii_lowercase()
            }
        );
    }
    assert!(matches!(
        check_output_mode("sideways"),
        Err(MicroBatchError::Catalog(_))
    ));
}

#[tokio::test]
async fn the_driver_runs_the_template_over_each_batch() {
    let fixture = Fixture::new().await;
    fixture.insert(SOURCE, "(1), (2), (3)").await;
    fixture.insert(SOURCE, "(4), (5)").await;
    let frame = stream(&fixture).await;
    let projected = frame
        .filter(ident("id").gt(lit(2_i64)))
        .expect("filter")
        .select(vec![(col("id") * lit(10_i64)).alias("id")])
        .expect("select");
    let template = PlanTemplate::from_frame(&projected).expect("template");
    let mut spec = StreamSpec::new(
        SOURCE,
        options(&[("streaming-max-files-per-micro-batch", "1")]),
        SinkSpec::Table {
            sink: SINK.to_string(),
        },
    );
    spec.plan = Some(template.clone());
    spec.trigger = Trigger::AvailableNow;
    let handle = started(&fixture, spec).await;
    assert_eq!(handle.await_termination(None).await, Ok(true));
    assert_eq!(fixture.ids(SINK).await, [30, 40, 50]);
    let batches: Vec<(u64, u64)> = handle
        .recent_progress()
        .iter()
        .map(|progress| (progress.batch_id.get(), progress.num_input_rows))
        .collect();
    assert_eq!(batches, [(0, 3), (1, 2)]);
    let mut mismatched = StreamSpec::new(
        "ice.sales.other",
        options(&[]),
        SinkSpec::Table {
            sink: SINK.to_string(),
        },
    );
    mismatched.plan = Some(template);
    let error = crate::microbatch::driver::StreamingQueryManager::of(&fixture.session)
        .register(&fixture.session, mismatched)
        .await
        .expect_err("the frame and the source disagree");
    assert!(matches!(error, MicroBatchError::Catalog(_)));
}
