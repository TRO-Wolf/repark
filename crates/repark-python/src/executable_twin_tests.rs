use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch, StringArray};
use arrow::datatypes::Float64Type;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, Result};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;
use pyo3::prelude::*;
use repark_core::frame_names::{attribute_ids, stamp};
use repark_core::{ReparkSession, SessionExtension};

use crate::dataframe::PyDataFrame;
use crate::session::PyReparkSession;

fn carries_id(plan: &LogicalPlan) -> bool {
    let mut keyed = false;
    plan.apply_with_subqueries(|node| {
        keyed = attribute_ids(node.schema()).iter().any(Option::is_some);
        Ok(if keyed {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })
    .expect("the probe walk is infallible");
    keyed
}

#[derive(Debug)]
struct KeyedPlanProbe {
    seen: Arc<AtomicUsize>,
}

impl AnalyzerRule for KeyedPlanProbe {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        if carries_id(&plan) {
            self.seen.fetch_add(1, Ordering::SeqCst);
        }
        Ok(plan)
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "keyed_plan_probe"
    }
}

struct ProbeExtension {
    seen: Arc<AtomicUsize>,
}

impl SessionExtension for ProbeExtension {
    fn configure_analyzer_rules(
        &self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        let mut ordered: Vec<Arc<dyn AnalyzerRule + Send + Sync>> =
            vec![Arc::new(KeyedPlanProbe {
                seen: Arc::clone(&self.seen),
            })];
        ordered.extend(rules);
        Ok(ordered)
    }
}

struct Probed {
    session: PyReparkSession,
    frame: PyDataFrame,
    seen: Arc<AtomicUsize>,
}

impl Probed {
    fn new(columns: Vec<(&str, ArrayRef)>) -> Self {
        let seen = Arc::new(AtomicUsize::new(0));
        let session = ReparkSession::builder()
            .with_extension(Arc::new(ProbeExtension {
                seen: Arc::clone(&seen),
            }))
            .build()
            .expect("a probed session builds");
        let runtime = crate::session::shared_runtime().expect("the shared runtime starts");
        let batch = RecordBatch::try_from_iter(columns).expect("the source batch builds");
        let (state, plan) = session
            .context()
            .read_batch(batch)
            .expect("the source reads")
            .into_parts();
        let stamped = stamp(plan).expect("the source stamps");
        assert!(carries_id(&stamped));
        let frame = PyDataFrame::new(DataFrame::new(state, stamped), Arc::clone(&runtime));
        Self {
            session: PyReparkSession { session, runtime },
            frame,
            seen,
        }
    }

    fn ints() -> Self {
        Self::new(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "v",
                Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef,
            ),
        ])
    }

    fn texts() -> Self {
        Self::new(vec![
            (
                "p",
                Arc::new(StringArray::from(vec!["x", "x", "y"])) as ArrayRef,
            ),
            (
                "s",
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ),
        ])
    }

    fn features() -> Self {
        let rows = [[0.0, 0.0], [1.0, 0.5], [5.0, 4.0], [6.0, 5.5]];
        let features = ListArray::from_iter_primitive::<Float64Type, _, _>(
            rows.iter().map(|row| Some(row.iter().copied().map(Some))),
        );
        Self::new(vec![
            ("features", Arc::new(features) as ArrayRef),
            (
                "label",
                Arc::new(Float64Array::from(vec![0.0, 0.0, 1.0, 1.0])) as ArrayRef,
            ),
        ])
    }

    fn handle(&self, py: Python<'_>) -> Py<PyDataFrame> {
        let frame = PyDataFrame::new(
            self.frame.inner().clone(),
            Arc::clone(&self.session.runtime),
        );
        Py::new(py, frame).expect("the frame wraps")
    }

    fn executions_saw_ids(&self) -> usize {
        self.seen.load(Ordering::SeqCst)
    }

    fn assert_clean_then_probe_live(&self, door: &str) {
        assert_eq!(
            self.executions_saw_ids(),
            0,
            "{door} must analyze the id-free twin, never the stamped plan"
        );
        let state = self.session.session.context().state();
        repark_functions::analyze_eagerly(&state, self.frame.inner().logical_plan().clone())
            .expect("the stamped plan analyzes");
        assert!(
            self.executions_saw_ids() > 0,
            "control: the probe sees a stamped plan"
        );
        assert!(carries_id(self.frame.inner().logical_plan()));
    }
}

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "repark_twin_{name}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos())
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn the_twin_is_built_once_per_handle_and_carries_no_id() {
    let probed = Probed::ints();
    let first = probed.frame.executable().expect("the twin builds");
    let second = probed.frame.executable().expect("the twin is cached");
    assert!(!carries_id(first.logical_plan()));
    assert!(std::ptr::eq(
        first.logical_plan().inputs()[0],
        second.logical_plan().inputs()[0]
    ));
    let other = Probed::ints();
    let other_twin = other.frame.executable().expect("the other twin builds");
    assert_eq!(
        other_twin.logical_plan().display_indent().to_string(),
        first.logical_plan().display_indent().to_string()
    );
    assert!(!std::ptr::eq(
        first.logical_plan().inputs()[0],
        other_twin.logical_plan().inputs()[0]
    ));
    assert_ne!(
        attribute_ids(probed.frame.inner().schema()),
        attribute_ids(other.frame.inner().schema())
    );
    assert!(carries_id(probed.frame.inner().logical_plan()));
}

#[test]
fn export_executes_the_twin() {
    let probed = Probed::ints();
    Python::attach(|py| {
        let capsule = probed
            .frame
            .__arrow_c_stream__(py, None)
            .expect("the export opens");
        drop(capsule);
    });
    probed.assert_clean_then_probe_live("__arrow_c_stream__");
}

#[test]
fn analyzed_schema_analyzes_the_twin() {
    let probed = Probed::ints();
    let schema = probed
        .frame
        .analyzed_arrow_schema_native()
        .expect("the schema analyzes");
    let logical = DFSchema::try_from(schema.as_ref().clone()).expect("the schema converts");
    assert!(attribute_ids(&logical).iter().all(Option::is_none));
    probed.assert_clean_then_probe_live("analyzed_arrow_schema_native");
}

#[test]
fn count_executes_the_twin() {
    let probed = Probed::ints();
    let rows = Python::attach(|py| probed.frame.count(py)).expect("count runs");
    assert_eq!(rows, 3);
    probed.assert_clean_then_probe_live("count");
}

#[test]
fn show_executes_the_twin() {
    let probed = Probed::ints();
    let shown = Python::attach(|py| probed.frame.show(py, 20)).expect("show runs");
    assert!(shown.contains("30"));
    probed.assert_clean_then_probe_live("show");
}

#[test]
fn input_files_plans_the_twin() {
    let probed = Probed::ints();
    let files = Python::attach(|py| crate::plan_introspect::input_files(py, &probed.frame))
        .expect("input_files plans");
    assert!(files.is_empty());
    probed.assert_clean_then_probe_live("input_files");
}

#[test]
fn temp_view_materialize_executes_the_twin() {
    let probed = Probed::ints();
    Python::attach(|py| {
        probed
            .session
            .materialize_as_temp_view(py, "twin_temp", &probed.frame)
    })
    .expect("the temp view materializes");
    probed.assert_clean_then_probe_live("materialize_as_temp_view");
}

#[test]
fn cache_view_materialize_executes_the_twin() {
    let probed = Probed::ints();
    Python::attach(|py| {
        probed
            .session
            .materialize_as_cache_view(py, "twin_cache", &probed.frame, (None, None))
    })
    .expect("the cache view materializes");
    probed.assert_clean_then_probe_live("materialize_as_cache_view");
}

#[test]
fn text_write_executes_the_twin() {
    let probed = Probed::new(vec![(
        "s",
        Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
    )]);
    let dir = scratch_dir("text");
    crate::text_io::write_text_frame(&probed.frame, &dir.to_string_lossy(), None)
        .expect("the text write runs");
    let _ = std::fs::remove_dir_all(&dir);
    probed.assert_clean_then_probe_live("write_text_frame");
}

#[test]
fn partitioned_text_write_executes_the_twin() {
    let probed = Probed::texts();
    let dir = scratch_dir("text_partitioned");
    crate::text_io::write_text_partitioned(
        &probed.frame,
        &dir.to_string_lossy(),
        vec!["p".to_string()],
        None,
        "UTC",
    )
    .expect("the partitioned text write runs");
    let _ = std::fs::remove_dir_all(&dir);
    probed.assert_clean_then_probe_live("write_text_partitioned");
}

#[test]
fn linear_regression_fit_executes_the_twin() {
    let probed = Probed::features();
    Python::attach(|py| {
        let frame = probed.handle(py);
        crate::ml::fit_linear_regression(
            py,
            frame.borrow(py),
            "features".to_string(),
            "label".to_string(),
            true,
            0.0,
            false,
        )
        .map(drop)
    })
    .expect("the linear fit runs");
    probed.assert_clean_then_probe_live("fit_linear_regression");
}

#[test]
fn logistic_regression_fit_executes_the_twin() {
    let probed = Probed::features();
    Python::attach(|py| {
        let frame = probed.handle(py);
        crate::ml::fit_logistic_regression(
            py,
            frame.borrow(py),
            "features".to_string(),
            "label".to_string(),
            true,
            20,
            1e-6,
        )
        .map(drop)
    })
    .expect("the logistic fit runs");
    probed.assert_clean_then_probe_live("fit_logistic_regression");
}

#[test]
fn kmeans_fit_executes_the_twin() {
    let probed = Probed::features();
    Python::attach(|py| {
        let frame = probed.handle(py);
        crate::ml::fit_kmeans(
            py,
            frame.borrow(py),
            "features".to_string(),
            2,
            5,
            7,
            "random",
        )
        .map(drop)
    })
    .expect("the kmeans fit runs");
    probed.assert_clean_then_probe_live("fit_kmeans");
}

#[test]
fn transpose_executes_the_twin() {
    let probed = Probed::ints();
    Python::attach(|py| {
        crate::dataframe_stats::transpose(&probed.frame, py, "id", vec!["v".to_string()], 100)
            .map(drop)
    })
    .expect("the transpose runs");
    probed.assert_clean_then_probe_live("transpose");
}
