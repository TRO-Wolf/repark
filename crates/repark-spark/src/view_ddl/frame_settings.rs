use std::any::Any;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{DFSchemaRef, internal_datafusion_err};
use datafusion::datasource::{ViewTable, provider_as_source};
use datafusion::error::Result;
use datafusion::logical_expr::{
    Expr, Extension, LogicalPlan, Projection, TableProviderFilterPushDown, TableScan, TableType,
    UNNAMED_TABLE, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{Analyzer, AnalyzerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionConfig};
use repark_core::extension::{FrameViewSeat, with_frame_view_seat};
use repark_core::frame_names;

use crate::view_ddl::read::{MAX_VIEW_EXPANSION_DEPTH, nested_depth_refusal};
use crate::view_ddl::temp_settings::{TempViewSettings, plan_under_overlay, view_filter_pushdown};
use crate::view_ddl::temp_view::replanning_scan;

pub(crate) const FRAME_SETTINGS_RULE: &str = "repark_frame_settings";
const NODE_NAME: &str = "FrameSettings";
const WRITER_V2: &str = "_repark_writer_v2_";

#[derive(Debug, Clone)]
struct FrameSettingsNode {
    input: LogicalPlan,
    settings: Arc<TempViewSettings>,
    analyzer: Arc<Analyzer>,
}

impl PartialEq for FrameSettingsNode {
    fn eq(&self, other: &Self) -> bool {
        self.settings == other.settings && self.input == other.input
    }
}

impl Eq for FrameSettingsNode {}

impl Hash for FrameSettingsNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.input.hash(state);
    }
}

impl PartialOrd for FrameSettingsNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.settings == other.settings {
            self.input.partial_cmp(&other.input)
        } else {
            None
        }
    }
}

impl UserDefinedLogicalNodeCore for FrameSettingsNode {
    fn name(&self) -> &str {
        NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.input.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "{NODE_NAME}")
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        let input = inputs
            .pop()
            .ok_or_else(|| internal_datafusion_err!("{NODE_NAME} takes one input"))?;
        Ok(Self {
            input,
            settings: Arc::clone(&self.settings),
            analyzer: Arc::clone(&self.analyzer),
        })
    }
}

#[derive(Debug)]
pub(crate) struct FrameView {
    plan: LogicalPlan,
    inlined: Option<LogicalPlan>,
    settings: Arc<TempViewSettings>,
    schema: SchemaRef,
}

impl FrameView {
    fn registered(
        plan: LogicalPlan,
        settings: Arc<TempViewSettings>,
        analyzer: Arc<Analyzer>,
    ) -> Self {
        let marker = FrameSettingsNode {
            input: plan.clone(),
            settings: Arc::clone(&settings),
            analyzer,
        };
        Self {
            schema: Arc::clone(plan.schema().inner()),
            inlined: Some(LogicalPlan::Extension(Extension {
                node: Arc::new(marker),
            })),
            plan,
            settings,
        }
    }

    fn closed(plan: LogicalPlan, settings: Arc<TempViewSettings>) -> Self {
        Self {
            schema: frame_names::strip_schema_ids(Arc::clone(plan.schema().inner())),
            inlined: None,
            plan,
            settings,
        }
    }
}

#[async_trait]
impl TableProvider for FrameView {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        Some(Cow::Borrowed(self.inlined.as_ref().unwrap_or(&self.plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(view_filter_pushdown(filters))
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match self.settings.overlay(state, state.config_options()) {
            Some(overlay) => {
                Box::pin(plan_under_overlay(
                    &overlay,
                    state,
                    self.plan.clone(),
                    projection,
                    filters,
                    limit,
                ))
                .await
            }
            None => {
                let view = ViewTable::new(self.plan.clone(), None);
                Box::pin(view.scan(state, projection, filters, limit)).await
            }
        }
    }
}

pub(crate) fn frame_view(
    name: &str,
    frame: &DataFrame,
    live: &ConfigOptions,
) -> Arc<dyn TableProvider> {
    let (state, plan) = frame.clone().into_parts();
    if replanning_scan(&plan) || scans_only(&plan) {
        return DataFrame::new(state, plan).into_view();
    }
    let captured = TempViewSettings::capture(state.config_options());
    let settings = Arc::new(if writer_v2(name) {
        captured.with_zone_of(live)
    } else {
        captured
    });
    let analyzer = Arc::new(state.analyzer().clone());
    Arc::new(FrameView::registered(plan, settings, analyzer))
}

fn scans_only(plan: &LogicalPlan) -> bool {
    let mut only = true;
    let walked = plan.apply_with_subqueries(|node| {
        only = matches!(
            node,
            LogicalPlan::TableScan(_) | LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_)
        );
        if only {
            node.apply_expressions(|expr| {
                let mut bare = expr;
                while let Expr::Alias(alias) = bare {
                    bare = alias.expr.as_ref();
                }
                only = matches!(bare, Expr::Column(_));
                Ok(if only {
                    TreeNodeRecursion::Continue
                } else {
                    TreeNodeRecursion::Stop
                })
            })?;
        }
        Ok(if only {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    });
    walked.is_ok() && only
}

fn writer_v2(name: &str) -> bool {
    name.rsplit('.')
        .next()
        .map(|tail| tail.trim_matches(|quote| quote == '"' || quote == '`'))
        .is_some_and(|tail| tail.starts_with(WRITER_V2))
}

pub(crate) fn install(config: SessionConfig) -> SessionConfig {
    with_frame_view_seat(
        config,
        FrameViewSeat::new(frame_view, Arc::new(FrameSettingsRule)),
    )
}

pub(crate) fn frame_plan(provider: &dyn TableProvider) -> Option<LogicalPlan> {
    let any: &dyn Any = provider;
    any.downcast_ref::<FrameView>()
        .map(|view| view.plan.clone())
}

fn closed_plan(plan: &LogicalPlan) -> Option<LogicalPlan> {
    let LogicalPlan::TableScan(scan) = plan else {
        return None;
    };
    let provider = datafusion::datasource::source_as_provider(&scan.source).ok()?;
    frame_plan(provider.as_ref())
}

pub(crate) fn opened(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let mut current = plan.clone();
    for _ in 0..=MAX_VIEW_EXPANSION_DEPTH {
        let next = current.transform_up_with_subqueries(|node| {
            Ok(match closed_plan(&node) {
                Some(inner) => Transformed::yes(inner),
                None => Transformed::no(node),
            })
        })?;
        if !next.transformed {
            return Ok(next.data);
        }
        current = next.data;
    }
    Err(nested_depth_refusal())
}

#[derive(Debug, Default)]
pub(crate) struct FrameSettingsRule;

impl AnalyzerRule for FrameSettingsRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if !holds_marker(&plan)? {
            return Ok(plan);
        }
        plan.transform_down_with_subqueries(|node| decide(node, config))
            .map(|decided| decided.data)
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        FRAME_SETTINGS_RULE
    }
}

fn marker(plan: &LogicalPlan) -> Option<&FrameSettingsNode> {
    match plan {
        LogicalPlan::Extension(extension) => extension.node.as_any().downcast_ref(),
        _ => None,
    }
}

fn holds_marker(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = marker(node).is_some();
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn decide(node: LogicalPlan, config: &ConfigOptions) -> Result<Transformed<LogicalPlan>> {
    let mut current = node;
    let mut unwrapped = false;
    while let Some(found) = marker(&current) {
        if !found.settings.same_as(config) {
            return closed_scan(found, config).map(Transformed::yes);
        }
        let input = found.input.clone();
        current = input;
        unwrapped = true;
    }
    Ok(Transformed::new_transformed(current, unwrapped))
}

fn closed_scan(found: &FrameSettingsNode, config: &ConfigOptions) -> Result<LogicalPlan> {
    let mut options = config.clone();
    found.settings.apply(&mut options);
    let analysed = found
        .analyzer
        .execute_and_check(found.input.clone(), &options, |_, _| {})?;
    let view = FrameView::closed(analysed, Arc::clone(&found.settings));
    let scan = TableScan::try_new(
        UNNAMED_TABLE,
        provider_as_source(Arc::new(view)),
        None,
        Vec::new(),
        None,
    )?;
    let columns = scan.projected_schema.columns();
    let wanted = Arc::clone(found.input.schema());
    let exprs = columns
        .into_iter()
        .zip(wanted.iter())
        .map(|(column, (qualifier, field))| {
            let metadata =
                (!field.metadata().is_empty()).then(|| FieldMetadata::from(field.as_ref()));
            Expr::Column(column).alias_qualified_with_metadata(
                qualifier.cloned(),
                field.name(),
                metadata,
            )
        })
        .collect::<Vec<_>>();
    Projection::try_new(exprs, Arc::new(LogicalPlan::TableScan(scan))).map(LogicalPlan::Projection)
}

#[cfg(test)]
mod tests;
