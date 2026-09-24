use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::{ViewTable, source_as_provider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::TableReference;
use iceberg::NamespaceIdent;
use repark_common::spark_error;
use repark_core::{CatalogRegistry, TempViewSession};

use crate::view_ddl::read::{
    MAX_VIEW_EXPANSION_DEPTH, TempHomes, nested_depth_refusal, plan_prepared_body,
    prepare_view_body_sql_with, refuse_write_query_body,
};

const MAX_TEMP_VIEW_WALK: usize = 4096;

pub(crate) struct TempViewDefinition {
    pub(crate) home: Vec<String>,
    pub(crate) display: String,
    pub(crate) sql: String,
    pub(crate) catalog: String,
    pub(crate) namespace: NamespaceIdent,
    pub(crate) aliases: Vec<(String, Option<String>)>,
}

pub(crate) struct ReplanningTempView {
    ctx: SessionContext,
    catalogs: CatalogRegistry,
    definition: TempViewDefinition,
    temp_homes: HashMap<String, Vec<String>>,
    references: Vec<Vec<String>>,
    schema: SchemaRef,
}

impl ReplanningTempView {
    pub(crate) fn display(&self) -> &str {
        &self.definition.display
    }
}

impl std::fmt::Debug for ReplanningTempView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReplanningTempView")
            .field("home", &self.definition.home)
            .field("sql", &self.definition.sql)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for ReplanningTempView {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let guard = self.catalogs.view_expansion_guard();
        if guard.level() > MAX_VIEW_EXPANSION_DEPTH {
            return Err(nested_depth_refusal());
        }
        let frame = plan_definition(
            &self.ctx,
            &self.catalogs,
            &self.definition,
            &TempHomes::Captured(&self.temp_homes),
        )
        .await?;
        let frame = conform_to_creation_schema(frame, &self.schema, &self.definition.display)?;
        ViewTable::new(frame.logical_plan().clone(), None)
            .scan(state, projection, filters, limit)
            .await
    }
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn replanning_temp_view(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    definition: TempViewDefinition,
    session: &dyn TempViewSession,
) -> Result<Arc<ReplanningTempView>> {
    let captured = Mutex::new(HashMap::new());
    let frame = plan_definition(
        ctx,
        catalogs,
        &definition,
        &TempHomes::Live {
            session,
            captured: &captured,
        },
    )
    .await?;
    let references = direct_temp_view_references(frame.logical_plan())?;
    let temp_homes = captured
        .into_inner()
        .unwrap_or_else(PoisonError::into_inner);
    Ok(Arc::new(ReplanningTempView {
        ctx: ctx.clone(),
        catalogs: catalogs.clone(),
        schema: Arc::new(frame.schema().as_arrow().clone()),
        definition,
        temp_homes,
        references,
    }))
}

#[allow(clippy::missing_errors_doc)]
pub(crate) async fn refuse_recursive_temp_view(
    ctx: &SessionContext,
    view: &ReplanningTempView,
) -> Result<()> {
    let target = &view.definition.home;
    let mut pending = view
        .references
        .iter()
        .map(|home| (home.clone(), vec![view.definition.display.clone()]))
        .collect::<Vec<_>>();
    let mut visited: HashSet<Vec<String>> = HashSet::new();
    while let Some((home, path)) = pending.pop() {
        if visited.len() > MAX_TEMP_VIEW_WALK {
            return Err(nested_depth_refusal());
        }
        if &home == target {
            let mut cycle = path;
            cycle.push(view.definition.display.clone());
            let cycle = cycle.join(" -> ");
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::RECURSIVE_VIEW,
                &[
                    ("viewIdent", view.definition.display.as_str()),
                    ("newPath", cycle.as_str()),
                ],
            )));
        }
        if !visited.insert(home.clone()) {
            continue;
        }
        let Some(current) = registered_temp_view(ctx, &home).await? else {
            continue;
        };
        for next in &current.references {
            let mut next_path = path.clone();
            next_path.push(current.display.clone());
            pending.push((next.clone(), next_path));
        }
    }
    Ok(())
}

struct RegisteredTempView {
    display: String,
    references: Vec<Vec<String>>,
}

async fn registered_temp_view(
    ctx: &SessionContext,
    home: &[String],
) -> Result<Option<RegisteredTempView>> {
    let [catalog, schema, table] = home else {
        return Ok(None);
    };
    let reference = TableReference::full(catalog.as_str(), schema.as_str(), table.as_str());
    if !ctx.table_exist(reference.clone())? {
        return Ok(None);
    }
    let provider = ctx.table_provider(reference).await?;
    if let Some(view) = registered_view_summary(provider.as_ref()) {
        return Ok(Some(view));
    }
    let Some(plan) = provider.get_logical_plan() else {
        return Ok(None);
    };
    let LogicalPlan::TableScan(scan) = plan.as_ref() else {
        return Ok(None);
    };
    let inner = source_as_provider(&scan.source)?;
    Ok(registered_view_summary(inner.as_ref()))
}

fn registered_view_summary(provider: &dyn TableProvider) -> Option<RegisteredTempView> {
    let provider: &dyn Any = provider;
    provider
        .downcast_ref::<ReplanningTempView>()
        .map(|view| RegisteredTempView {
            display: view.definition.display.clone(),
            references: view.references.clone(),
        })
}

fn direct_temp_view_references(plan: &LogicalPlan) -> Result<Vec<Vec<String>>> {
    let mut references: Vec<Vec<String>> = Vec::new();
    let mut pending = vec![plan.clone()];
    let mut visits = 0_usize;
    while let Some(next) = pending.pop() {
        visits += 1;
        if visits > MAX_TEMP_VIEW_WALK {
            return Err(nested_depth_refusal());
        }
        next.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Ok(provider) = source_as_provider(&scan.source)
            {
                let any: &dyn Any = provider.as_ref();
                if let Some(view) = any.downcast_ref::<ReplanningTempView>() {
                    if !references.contains(&view.definition.home) {
                        references.push(view.definition.home.clone());
                    }
                } else if let Some(inner) = provider.get_logical_plan() {
                    pending.push(inner.into_owned());
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(references)
}

async fn plan_definition(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    definition: &TempViewDefinition,
    temp_homes: &TempHomes<'_>,
) -> Result<DataFrame> {
    refuse_write_query_body(&definition.sql)?;
    let (sql, pins) = prepare_view_body_sql_with(
        ctx,
        catalogs,
        &definition.catalog,
        &definition.namespace,
        &definition.sql,
        temp_homes,
    )
    .await?;
    let frame = plan_prepared_body(ctx, catalogs, &sql, &pins).await?;
    apply_temp_view_aliases(frame, &definition.aliases, &definition.display)
}

fn backticked(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn apply_temp_view_aliases(
    frame: DataFrame,
    aliases: &[(String, Option<String>)],
    display: &str,
) -> Result<DataFrame> {
    if aliases.is_empty() {
        return Ok(frame);
    }
    let columns = frame.schema().columns();
    if aliases.len() != columns.len() {
        let view_columns = aliases
            .iter()
            .map(|(alias, _)| backticked(alias))
            .collect::<Vec<_>>()
            .join(", ");
        let data_columns = columns
            .iter()
            .map(|column| backticked(&column.name))
            .collect::<Vec<_>>()
            .join(", ");
        let condition = if aliases.len() > columns.len() {
            spark_error::CREATE_VIEW_COLUMN_ARITY_MISMATCH_NOT_ENOUGH_DATA_COLUMNS
        } else {
            spark_error::CREATE_VIEW_COLUMN_ARITY_MISMATCH_TOO_MANY_DATA_COLUMNS
        };
        return Err(DataFusionError::Plan(spark_error::message(
            condition,
            &[
                ("viewName", display),
                ("viewColumns", view_columns.as_str()),
                ("dataColumns", data_columns.as_str()),
            ],
        )));
    }
    let projection = columns
        .into_iter()
        .zip(aliases)
        .map(|(column, (alias, _))| Expr::Column(column).alias(alias))
        .collect::<Vec<_>>();
    frame.select(projection)
}

fn conform_to_creation_schema(
    frame: DataFrame,
    creation: &SchemaRef,
    display: &str,
) -> Result<DataFrame> {
    let columns = frame.schema().columns();
    let fields = frame.schema().fields().clone();
    let unchanged = fields.len() == creation.fields().len()
        && fields
            .iter()
            .zip(creation.fields())
            .all(|(now, then)| now.name() == then.name() && now.data_type() == then.data_type());
    if unchanged {
        return Ok(frame);
    }
    let mut projection = Vec::with_capacity(creation.fields().len());
    for field in creation.fields() {
        let position = fields
            .iter()
            .position(|now| now.name() == field.name())
            .or_else(|| {
                fields
                    .iter()
                    .position(|now| now.name().eq_ignore_ascii_case(field.name()))
            })
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "temporary view {display} can no longer resolve its column {}: the objects \
                     its query reads changed shape since CREATE TEMPORARY VIEW; re-create the view",
                    backticked(field.name())
                ))
            })?;
        let (Some(column), Some(now)) = (columns.get(position), fields.get(position)) else {
            return Err(DataFusionError::Internal(
                "temporary view column lookup out of range".to_string(),
            ));
        };
        if now.data_type() != field.data_type() {
            return Err(DataFusionError::Plan(format!(
                "temporary view {display} column {} is now {} but was {} at CREATE TEMPORARY \
                 VIEW; re-create the view",
                backticked(field.name()),
                now.data_type(),
                field.data_type()
            )));
        }
        projection.push(Expr::Column(column.clone()).alias(field.name()));
    }
    frame.select(projection)
}
