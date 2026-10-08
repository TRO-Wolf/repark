use std::collections::BTreeMap;

use datafusion::functions_aggregate::expr_fn::{count, max};
use datafusion::logical_expr::{JoinType, ident, in_subquery, lit, scalar_subquery};
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

async fn fixed(fixture: &Fixture) -> DataFrame {
    fixture
        .session
        .sql("SELECT id AS key FROM ice.sales.other")
        .await
        .expect("a static frame")
}

fn joined(left: DataFrame, right: DataFrame, how: JoinType, stream_on_the_left: bool) -> DataFrame {
    let (left_key, right_key) = if stream_on_the_left {
        ("id", "key")
    } else {
        ("key", "id")
    };
    left.join(right, how, &[left_key], &[right_key], None)
        .expect("the join plans")
}

#[tokio::test]
async fn shapes_that_would_re_emit_the_static_side_refuse() {
    let fixture = Fixture::new().await;
    let frame = stream(&fixture).await;
    let other = fixed(&fixture).await;
    let union = "union of a streaming and a static DataFrame";
    let full = "full outer join of a streaming and a static DataFrame";
    assert_eq!(
        refused(&frame.clone().union(other.clone()).expect("union")),
        union
    );
    assert_eq!(
        refused(&other.clone().union(frame.clone()).expect("union")),
        union
    );
    for (how, stream_on_the_left, operator) in [
        (
            JoinType::Left,
            false,
            "left outer join with the static DataFrame on the left",
        ),
        (
            JoinType::Right,
            true,
            "right outer join with the static DataFrame on the right",
        ),
        (JoinType::Full, true, full),
        (JoinType::Full, false, full),
        (
            JoinType::LeftSemi,
            false,
            "left semi join with the streaming DataFrame on the right",
        ),
        (
            JoinType::LeftAnti,
            false,
            "left anti join with the streaming DataFrame on the right",
        ),
        (
            JoinType::RightSemi,
            true,
            "right semi join with the streaming DataFrame on the left",
        ),
        (
            JoinType::RightAnti,
            true,
            "right anti join with the streaming DataFrame on the left",
        ),
    ] {
        let plan = if stream_on_the_left {
            joined(frame.clone(), other.clone(), how, true)
        } else {
            joined(other.clone(), frame.clone(), how, false)
        };
        assert_eq!(refused(&plan), operator, "{how:?}");
    }
    let streamed = frame.clone().select(vec![col("id")]).expect("select");
    let membership = other
        .clone()
        .filter(in_subquery(
            col("key"),
            std::sync::Arc::new(streamed.logical_plan().clone()),
        ))
        .expect("filter");
    assert_eq!(refused(&membership), "a streaming DataFrame in a subquery");
    let error = PlanTemplate::from_frame(&frame.union(other).expect("union"))
        .expect_err("a union with a static frame");
    assert_eq!(
        error.to_string(),
        "union of a streaming and a static DataFrame is not supported on a streaming DataFrame; use foreachBatch"
    );
}

#[tokio::test]
async fn shapes_that_preserve_the_stream_run_spark_s_rows() {
    for (how, stream_on_the_left, landed) in [
        (JoinType::Inner, true, vec![2]),
        (JoinType::Inner, false, vec![2]),
        (JoinType::Left, true, vec![1, 2, 3, 4, 5]),
        (JoinType::Right, false, vec![1, 2, 3, 4, 5]),
        (JoinType::LeftSemi, true, vec![2]),
        (JoinType::LeftAnti, true, vec![1, 3, 4, 5]),
    ] {
        let fixture = Fixture::new().await;
        fixture.insert(SOURCE, "(1), (2), (3)").await;
        fixture.insert(SOURCE, "(4), (5)").await;
        fixture.insert("ice.sales.other", "(2), (900)").await;
        let frame = stream(&fixture).await;
        let other = fixed(&fixture).await;
        let plan = if stream_on_the_left {
            joined(frame, other, how, true)
        } else {
            joined(other, frame, how, false)
        };
        let plan = plan.select(vec![col("id")]).expect("select");
        let mut spec = StreamSpec::new(
            SOURCE,
            options(&[("streaming-max-files-per-micro-batch", "1")]),
            SinkSpec::Table {
                sink: SINK.to_string(),
            },
        );
        spec.plan = Some(PlanTemplate::from_frame(&plan).expect("the stream is preserved"));
        spec.trigger = Trigger::AvailableNow;
        let handle = started(&fixture, spec).await;
        assert_eq!(handle.await_termination(None).await, Ok(true), "{how:?}");
        assert_eq!(
            fixture.ids(SINK).await,
            landed,
            "{how:?} {stream_on_the_left}"
        );
    }
    let fixture = Fixture::new().await;
    let other = fixed(&fixture).await;
    let ceiling = other
        .aggregate(vec![], vec![max(col("key"))])
        .expect("a static aggregate");
    let bounded = stream(&fixture)
        .await
        .filter(col("id").lt(scalar_subquery(std::sync::Arc::new(
            ceiling.logical_plan().clone(),
        ))))
        .expect("filter");
    PlanTemplate::from_frame(&bounded).expect("a static scalar subquery");
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
