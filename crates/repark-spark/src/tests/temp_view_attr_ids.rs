use super::common::*;

use std::collections::HashSet;
use std::sync::Mutex;

use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::DataFrame;

use crate::write_options::StatementWriteOptions;

struct StubViews {
    registered: Mutex<Vec<(String, DataFrame)>>,
}

impl repark_core::TempViewSession for StubViews {
    fn create_or_replace_temp_view_from(
        &self,
        name: &str,
        frame: &DataFrame,
    ) -> repark_common::Result<()> {
        self.registered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((name.to_string(), frame.clone()));
        Ok(())
    }

    fn resolve_temp_view_home_ref(&self, name: &str) -> repark_common::Result<Option<Vec<String>>> {
        Ok(name.eq("tv").then(|| {
            vec![
                "datafusion".to_string(),
                "public".to_string(),
                "tv".to_string(),
            ]
        }))
    }

    fn temp_view_home(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["datafusion".to_string(), "public".to_string()])
    }

    fn list_temp_view_names(&self) -> repark_common::Result<Vec<String>> {
        Ok(vec!["tv".to_string()])
    }

    fn drop_temp_view(&self, _name: &str) -> repark_common::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn sql_temp_view_over_a_stamped_frame_registers_a_clean_scan_with_no_carried_ids() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (ctx, catalogs) = setup(&warehouse).await;
    let source = ctx
        .sql("SELECT CAST(1 AS BIGINT) AS id, CAST(10 AS BIGINT) AS v")
        .await
        .expect("source plans");
    let (state, plan) = source.into_parts();
    let stamped = DataFrame::new(
        state,
        repark_core::frame_names::stamp(plan).expect("source stamps"),
    );
    let wanted = repark_core::frame_names::attribute_ids(stamped.schema());
    assert!(wanted.iter().all(Option::is_some));
    ctx.register_table("tv", stamped.into_view())
        .expect("tv registers");
    let stub = StubViews {
        registered: Mutex::new(Vec::new()),
    };
    crate::router::execute_in_session(
        &ctx,
        &catalogs,
        "CREATE TEMPORARY VIEW sv AS SELECT * FROM tv",
        &HashSet::new(),
        &StatementWriteOptions::empty(),
        Some(&stub),
    )
    .await
    .expect("sv registers");
    let registered = stub
        .registered
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    assert_eq!(registered.len(), 1);
    assert_eq!(registered[0].0, "sv");
    let frame = registered[0].1.clone();
    assert!(
        repark_core::frame_names::attribute_ids(frame.schema())
            .iter()
            .all(Option::is_none)
    );
    let mut scans = Vec::new();
    frame
        .logical_plan()
        .apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                scans.push((scan.projected_schema.clone(), scan.source.schema()));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk scans");
    assert_eq!(scans.len(), 1);
    for (projected, source) in &scans {
        assert!(
            repark_core::frame_names::attribute_ids(projected)
                .iter()
                .all(Option::is_none)
        );
        let provider = DFSchema::try_from(source.as_ref().clone()).expect("provider schema");
        assert!(
            repark_core::frame_names::attribute_ids(&provider)
                .iter()
                .all(Option::is_none)
        );
    }
    let (state, plan) = frame.into_parts();
    let stripped = DataFrame::new(
        state,
        repark_core::frame_names::strip(plan).expect("a clean scan strips"),
    );
    let batches = stripped.collect().await.expect("stripped sv collects");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ids");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("values");
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), values.value(index)));
        }
    }
    assert_eq!(rows, vec![(1, 10)]);
}
