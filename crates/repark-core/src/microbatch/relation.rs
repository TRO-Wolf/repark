use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session as CatalogSession;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::{TableProvider, TableType, provider_as_source, source_as_provider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{JoinType, LogicalPlan, LogicalPlanBuilder, TableScan};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, Expr};
use datafusion::sql::TableReference;
use repark_iceberg::microbatch::error::MicroBatchError;

use crate::Session;
use crate::time_travel::microbatch_source::{MicroBatchSource, SourceBatch, SourceOptions};

pub const STREAMING_SCAN_REFUSAL: &str =
    "Queries with streaming sources must be executed with writeStream.start()";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingRelation {
    source: String,
    options: BTreeMap<String, String>,
    schema: SchemaRef,
}

impl StreamingRelation {
    #[must_use]
    pub fn source(&self) -> &str {
        &self.source
    }

    #[must_use]
    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.options
    }
}

#[async_trait]
impl TableProvider for StreamingRelation {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn CatalogSession,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::External(Box::new(
            MicroBatchError::Catalog(format!(
                "{STREAMING_SCAN_REFUSAL}; {source} is a streaming source",
                source = self.source
            )),
        )))
    }
}

#[allow(clippy::missing_errors_doc)]
pub async fn streaming_frame(
    session: &Session,
    table: &str,
    options: &BTreeMap<String, String>,
) -> Result<DataFrame, MicroBatchError> {
    let parsed = SourceOptions::from_options(options)?;
    let source = MicroBatchSource::open(session, table, parsed).await?;
    let relation = StreamingRelation {
        source: table.to_string(),
        options: options.clone(),
        schema: source.arrow_schema()?,
    };
    let plan = LogicalPlanBuilder::scan(
        TableReference::parse_str(table),
        provider_as_source(Arc::new(relation)),
        None,
    )
    .and_then(LogicalPlanBuilder::build)
    .map_err(|error| plan_error(&error))?;
    Ok(DataFrame::new(session.context().state(), plan))
}

#[derive(Clone)]
pub struct PlanTemplate {
    plan: LogicalPlan,
    relation: StreamingRelation,
}

impl fmt::Debug for PlanTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlanTemplate")
            .field("source", &self.relation.source)
            .finish_non_exhaustive()
    }
}

impl PlanTemplate {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_frame(frame: &DataFrame) -> Result<PlanTemplate, MicroBatchError> {
        let plan = frame.logical_plan().clone();
        refuse_stateful(&plan)?;
        let relations = streaming_relations(&plan);
        let relation = match relations.as_slice() {
            [relation] => relation.clone(),
            [] => {
                return Err(MicroBatchError::Catalog(String::from(
                    "writeStream needs a streaming DataFrame (readStream.format(\"iceberg\").load(...))",
                )));
            }
            _ => {
                return Err(MicroBatchError::FeatureRefused {
                    feature: String::from("more than one streaming source in one query"),
                });
            }
        };
        Ok(PlanTemplate { plan, relation })
    }

    #[must_use]
    pub fn source(&self) -> &str {
        &self.relation.source
    }

    #[must_use]
    pub fn source_options(&self) -> &BTreeMap<String, String> {
        &self.relation.options
    }

    #[must_use]
    pub fn explain(&self) -> String {
        let caps: Vec<String> = self
            .relation
            .options
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect();
        format!(
            "StreamingRelation iceberg {source} [{caps}]\n{plan}",
            source = self.relation.source,
            caps = caps.join(", "),
            plan = self.plan.display_indent()
        )
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn bind(&self, batch: &SourceBatch) -> Result<DataFrame, MicroBatchError> {
        let (state, batch_plan) = batch.frame.clone().into_parts();
        let mut source = None;
        batch_plan
            .apply(|node| {
                if let LogicalPlan::TableScan(scan) = node {
                    source = Some(Arc::clone(&scan.source));
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
            .map_err(|error| plan_error(&error))?;
        let source = source.ok_or_else(|| {
            MicroBatchError::Catalog(String::from("the batch frame reads no table"))
        })?;
        if source.schema() != self.relation.schema {
            return Err(MicroBatchError::Catalog(format!(
                "the batch schema of {name} no longer matches the streaming frame's; restart the query",
                name = self.relation.source
            )));
        }
        let bound = self
            .plan
            .clone()
            .transform_up(|node| match node {
                LogicalPlan::TableScan(scan) if is_streaming(&scan) => {
                    Ok(Transformed::yes(LogicalPlan::TableScan(TableScan {
                        source: Arc::clone(&source),
                        ..scan
                    })))
                }
                other => Ok(Transformed::no(other)),
            })
            .map_err(|error| plan_error(&error))?
            .data;
        Ok(DataFrame::new(state, bound))
    }
}

#[must_use]
pub fn is_streaming_frame(frame: &DataFrame) -> bool {
    streams(frame.logical_plan())
}

#[allow(clippy::missing_errors_doc)]
pub fn check_output_mode(mode: &str) -> Result<(), MicroBatchError> {
    match mode.to_ascii_lowercase().as_str() {
        "append" => Ok(()),
        "complete" | "update" => Err(MicroBatchError::OutputModeRefused {
            mode: mode.to_ascii_lowercase(),
        }),
        _ => Err(MicroBatchError::Catalog(format!(
            "Unknown output mode {mode}. Accepted output modes are 'append', 'complete', 'update'"
        ))),
    }
}

fn is_streaming(scan: &TableScan) -> bool {
    streaming_relation_of(scan).is_some()
}

fn streaming_relation_of(scan: &TableScan) -> Option<StreamingRelation> {
    source_as_provider(&scan.source)
        .ok()
        .and_then(|provider| provider.downcast_ref::<StreamingRelation>().cloned())
}

fn streaming_relations(plan: &LogicalPlan) -> Vec<StreamingRelation> {
    let mut found = Vec::new();
    let _ = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Some(relation) = streaming_relation_of(scan)
        {
            found.push(relation);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn streams(plan: &LogicalPlan) -> bool {
    !streaming_relations(plan).is_empty()
}

fn stateful_operator(node: &LogicalPlan) -> Option<&'static str> {
    match node {
        LogicalPlan::Aggregate(_) => Some("aggregation"),
        LogicalPlan::Distinct(_) => Some("dropDuplicates"),
        LogicalPlan::Sort(_) => Some("sort"),
        LogicalPlan::Limit(_) => Some("limit"),
        LogicalPlan::Window(_) => Some("window function"),
        LogicalPlan::Join(join) if streams(&join.left) && streams(&join.right) => {
            Some("stream-stream join")
        }
        _ => None,
    }
}

fn static_side_operator(node: &LogicalPlan) -> Option<&'static str> {
    match node {
        LogicalPlan::Union(union) => {
            let streaming = union.inputs.iter().filter(|input| streams(input)).count();
            (streaming < union.inputs.len())
                .then_some("union of a streaming and a static DataFrame")
        }
        LogicalPlan::Join(join) => {
            let stream_on_the_left = streams(&join.left);
            match join.join_type {
                JoinType::Inner => None,
                JoinType::Full => Some("full outer join of a streaming and a static DataFrame"),
                JoinType::Left => (!stream_on_the_left)
                    .then_some("left outer join with the static DataFrame on the left"),
                JoinType::Right => stream_on_the_left
                    .then_some("right outer join with the static DataFrame on the right"),
                JoinType::LeftSemi | JoinType::LeftMark => (!stream_on_the_left)
                    .then_some("left semi join with the streaming DataFrame on the right"),
                JoinType::LeftAnti => (!stream_on_the_left)
                    .then_some("left anti join with the streaming DataFrame on the right"),
                JoinType::RightSemi | JoinType::RightMark => stream_on_the_left
                    .then_some("right semi join with the streaming DataFrame on the left"),
                JoinType::RightAnti => stream_on_the_left
                    .then_some("right anti join with the streaming DataFrame on the left"),
            }
        }
        _ => None,
    }
}

fn streams_in_a_subquery(node: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = node.apply_subqueries(|subquery| {
        found |= streams(subquery);
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn refused_operator(node: &LogicalPlan) -> Option<&'static str> {
    if !streams(node) {
        return None;
    }
    stateful_operator(node)
        .or_else(|| static_side_operator(node))
        .or_else(|| streams_in_a_subquery(node).then_some("a streaming DataFrame in a subquery"))
}

fn refuse_stateful(plan: &LogicalPlan) -> Result<(), MicroBatchError> {
    let mut refused = None;
    let _ = plan.apply_with_subqueries(|node| {
        if let Some(operator) = refused_operator(node) {
            refused = Some(operator);
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    match refused {
        Some(operator) => Err(MicroBatchError::StatefulOperatorRefused {
            operator: operator.to_string(),
        }),
        None => Ok(()),
    }
}

fn plan_error(error: &DataFusionError) -> MicroBatchError {
    MicroBatchError::Catalog(repark_common::redaction::mask_value_credentials(
        &error.to_string(),
    ))
}

#[cfg(test)]
#[path = "relation_tests.rs"]
mod tests;
