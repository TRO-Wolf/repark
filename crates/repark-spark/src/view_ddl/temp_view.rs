use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
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
use repark_core::column_resolution::on_grown_stack_with;
use repark_core::{CatalogRegistry, TempViewSession};

use crate::view_ddl::read::{
    MAX_VIEW_EXPANSION_DEPTH, TempHomes, VIEW_EXPANSION_STACK_RED_ZONE,
    VIEW_EXPANSION_STACK_SEGMENT, nested_depth_refusal, plan_prepared_body,
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
    dependencies: Vec<Vec<String>>,
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
        on_grown_stack_with(
            VIEW_EXPANSION_STACK_RED_ZONE,
            VIEW_EXPANSION_STACK_SEGMENT,
            self.scan_replanned(state, projection, filters, limit),
        )
        .await
    }
}

impl ReplanningTempView {
    async fn scan_replanned(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let guard = self.catalogs.view_expansion_guard();
        if guard.level() > MAX_VIEW_EXPANSION_DEPTH {
            let depth = MAX_VIEW_EXPANSION_DEPTH.to_string();
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::VIEW_EXCEED_MAX_NESTED_DEPTH,
                &[
                    ("viewName", self.definition.display.as_str()),
                    ("maxNestedDepth", depth.as_str()),
                ],
            )));
        }
        refuse_dropped_dependency(&self.ctx, &self.dependencies)?;
        let frame = Box::pin(plan_definition(
            &self.ctx,
            &self.catalogs,
            &self.definition,
            &TempHomes::Captured(&self.temp_homes),
        ))
        .await?;
        let frame = conform_to_creation_schema(frame, &self.schema, &self.definition.display)?;
        let view = ViewTable::new(frame.logical_plan().clone(), None);
        Box::pin(view.scan(state, projection, filters, limit)).await
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
    let mut dependencies = temp_home_dependencies(frame.logical_plan(), &definition.home)?;
    for reference in &references {
        if !dependencies.contains(reference) {
            dependencies.push(reference.clone());
        }
    }
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
        dependencies,
    }))
}

fn temp_home_dependencies(plan: &LogicalPlan, home: &[String]) -> Result<Vec<Vec<String>>> {
    let [catalog, schema, ..] = home else {
        return Ok(Vec::new());
    };
    let mut dependencies: Vec<Vec<String>> = Vec::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let TableReference::Full {
                catalog: scan_catalog,
                schema: scan_schema,
                table,
            } = &scan.table_name
            && scan_catalog.as_ref() == catalog.as_str()
            && scan_schema.as_ref() == schema.as_str()
        {
            let dependency = vec![catalog.clone(), schema.clone(), table.to_string()];
            if !dependencies.contains(&dependency) {
                dependencies.push(dependency);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(dependencies)
}

fn refuse_dropped_dependency(ctx: &SessionContext, dependencies: &[Vec<String>]) -> Result<()> {
    for dependency in dependencies {
        let [catalog, schema, table] = dependency.as_slice() else {
            continue;
        };
        let reference = TableReference::full(catalog.as_str(), schema.as_str(), table.as_str());
        if !ctx.table_exist(reference)? {
            let relation = backticked(table);
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::TABLE_OR_VIEW_NOT_FOUND,
                &[("relationName", relation.as_str())],
            )));
        }
    }
    Ok(())
}

pub(crate) fn temp_view_column_comments(
    provider: &dyn TableProvider,
) -> Result<Option<Vec<Option<String>>>> {
    let comments = |view: &ReplanningTempView| {
        let aliases = &view.definition.aliases;
        view.schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, _)| aliases.get(index).and_then(|(_, comment)| comment.clone()))
            .collect::<Vec<_>>()
    };
    let any: &dyn Any = provider;
    if let Some(view) = any.downcast_ref::<ReplanningTempView>() {
        return Ok(Some(comments(view)));
    }
    let Some(plan) = provider.get_logical_plan() else {
        return Ok(None);
    };
    let LogicalPlan::TableScan(scan) = plan.as_ref() else {
        return Ok(None);
    };
    let inner = source_as_provider(&scan.source)?;
    let any: &dyn Any = inner.as_ref();
    Ok(any.downcast_ref::<ReplanningTempView>().map(comments))
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
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
        {
            let any: &dyn Any = provider.as_ref();
            if let Some(view) = any.downcast_ref::<ReplanningTempView>()
                && !references.contains(&view.definition.home)
            {
                references.push(view.definition.home.clone());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
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
    let pairs = frame
        .schema()
        .columns()
        .into_iter()
        .zip(frame.schema().fields().iter().cloned())
        .collect::<Vec<_>>();
    let unchanged = pairs.len() == creation.fields().len()
        && pairs.iter().zip(creation.fields()).all(|((_, now), then)| {
            now.name() == then.name() && now.data_type() == then.data_type()
        });
    if unchanged {
        return Ok(frame);
    }
    let mut projection = Vec::with_capacity(creation.fields().len());
    for field in creation.fields() {
        let matches = pairs
            .iter()
            .filter(|(_, now)| now.name().eq_ignore_ascii_case(field.name()))
            .collect::<Vec<_>>();
        let [(column, now)] = matches.as_slice() else {
            let actual = matches
                .iter()
                .map(|(_, now)| backticked(now.name()))
                .collect::<Vec<_>>()
                .join(", ");
            let actual = format!("[{actual}]");
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::INCOMPATIBLE_VIEW_SCHEMA_CHANGE,
                &[
                    ("viewName", display),
                    ("colName", field.name()),
                    ("expectedNum", "1"),
                    ("actualCols", actual.as_str()),
                    ("suggestion", "CREATE OR REPLACE TEMPORARY VIEW"),
                ],
            )));
        };
        let expr = Expr::Column(column.clone());
        let expr = if now.data_type() == field.data_type() {
            expr
        } else if can_up_cast(now.data_type(), field.data_type()) {
            datafusion::logical_expr::cast(expr, field.data_type().clone())
        } else {
            let expression = match &column.relation {
                Some(relation) => format!("{}.{}", relation.table(), column.name),
                None => column.name.clone(),
            };
            let source = spark_type_display(now.data_type());
            let target = spark_type_display(field.data_type());
            return Err(DataFusionError::Plan(spark_error::message(
                spark_error::CANNOT_UP_CAST_DATATYPE,
                &[
                    ("expression", expression.as_str()),
                    ("sourceType", source.as_str()),
                    ("targetType", target.as_str()),
                    ("details", UP_CAST_DETAILS),
                ],
            )));
        };
        projection.push(expr.alias(field.name()));
    }
    frame.select(projection)
}

const UP_CAST_DETAILS: &str = "The type path of the target object is:\n\nYou can either add an \
     explicit cast to the input data or choose a higher precision type of the field in the target \
     object";

fn spark_type_display(data_type: &DataType) -> String {
    format!(
        "\"{}\"",
        crate::spark_type_names::spark_ddl_type_name(data_type).to_ascii_uppercase()
    )
}

fn can_up_cast(from: &DataType, to: &DataType) -> bool {
    const NUMERIC_PRECEDENCE: [DataType; 6] = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    if matches!(from, DataType::Null) {
        return true;
    }
    if matches!(from, DataType::Date32) && matches!(to, DataType::Timestamp(_, _)) {
        return true;
    }
    let rank = |data_type: &DataType| {
        NUMERIC_PRECEDENCE
            .iter()
            .position(|candidate| candidate == data_type)
    };
    matches!((rank(from), rank(to)), (Some(from), Some(to)) if from < to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;

    const WALK_REFUSAL: &str = "Error during planning: [VIEW_NESTED_DEPTH_LIMIT] View resolution exceeded the maximum nested depth of 100";

    fn home(index: usize) -> Vec<String> {
        vec![
            "datafusion".to_string(),
            "public".to_string(),
            format!("v{index}"),
        ]
    }

    fn chained_view(
        ctx: &SessionContext,
        index: usize,
        references: Vec<Vec<String>>,
    ) -> ReplanningTempView {
        ReplanningTempView {
            ctx: ctx.clone(),
            catalogs: CatalogRegistry::new(),
            definition: TempViewDefinition {
                home: home(index),
                display: format!("`v{index}`"),
                sql: "SELECT 1".to_string(),
                catalog: String::new(),
                namespace: NamespaceIdent::new(String::new()),
                aliases: Vec::new(),
            },
            temp_homes: HashMap::new(),
            references,
            dependencies: Vec::new(),
            schema: Arc::new(Schema::empty()),
        }
    }

    fn register_chain(ctx: &SessionContext, length: usize) {
        for index in 0..length {
            let references = if index == 0 {
                Vec::new()
            } else {
                vec![home(index - 1)]
            };
            let view = chained_view(ctx, index, references);
            ctx.register_table(
                TableReference::full("datafusion", "public", format!("v{index}")),
                Arc::new(view),
            )
            .unwrap_or_else(|error| panic!("register v{index}: {error}"));
        }
    }

    #[test]
    fn references_are_found_through_an_inlined_temp_view_scan() {
        let ctx = SessionContext::new();
        let view = ctx
            .read_table(Arc::new(chained_view(&ctx, 7, Vec::new())))
            .unwrap_or_else(|error| panic!("scan the view: {error}"));
        let wrapped = ctx
            .read_table(view.into_view())
            .unwrap_or_else(|error| panic!("wrap the view: {error}"));
        assert!(matches!(
            wrapped.logical_plan(),
            LogicalPlan::SubqueryAlias(_)
        ));
        let references = direct_temp_view_references(wrapped.logical_plan())
            .unwrap_or_else(|error| panic!("walk: {error}"));
        assert_eq!(references, vec![home(7)]);
    }

    #[tokio::test]
    async fn cycle_walk_accepts_a_chain_within_its_visit_budget() {
        let ctx = SessionContext::new();
        register_chain(&ctx, MAX_TEMP_VIEW_WALK);
        let target = chained_view(&ctx, usize::MAX, vec![home(MAX_TEMP_VIEW_WALK - 1)]);
        refuse_recursive_temp_view(&ctx, &target)
            .await
            .unwrap_or_else(|error| panic!("chain within budget: {error}"));
    }

    #[tokio::test]
    async fn cycle_walk_refuses_past_its_visit_budget() {
        let ctx = SessionContext::new();
        register_chain(&ctx, MAX_TEMP_VIEW_WALK + 2);
        let target = chained_view(&ctx, usize::MAX, vec![home(MAX_TEMP_VIEW_WALK + 1)]);
        let error = refuse_recursive_temp_view(&ctx, &target)
            .await
            .err()
            .unwrap_or_else(|| panic!("chain past budget must refuse"));
        assert!(matches!(error, DataFusionError::Plan(_)), "{error:?}");
        assert_eq!(error.to_string(), WALK_REFUSAL);
    }
}
