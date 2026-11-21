use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion::common::{Statistics, internal_datafusion_err};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::table::Table;
use repark_core::extension::ReaderParseMemo;
use repark_iceberg::catalog::session_tables::{
    BindMode, Binding, HeldTable, TableState, binding, conformed, rebound_clone, rebound_provider,
    referenced_clone, same_state,
};

const WRITER_VIEW: &str = "_repark_writer_";
const INTERNAL_VIEWS: [&str; 3] = ["_repark_", "__repark_", "__rp_"];

#[allow(clippy::large_enum_variant)]
#[derive(Clone)]
enum Decided {
    Live(HeldTable),
    Dropped,
    Unknown,
}

pub(super) fn view_mode(name: &str) -> Option<BindMode> {
    let tail = name
        .rsplit('.')
        .next()
        .map(|tail| tail.trim_matches(|quote| quote == '"' || quote == '`'))?;
    if tail.starts_with(WRITER_VIEW) {
        return Some(BindMode::Command);
    }
    if INTERNAL_VIEWS.iter().any(|prefix| tail.starts_with(prefix)) {
        return None;
    }
    Some(BindMode::View)
}

pub(super) fn bound_for_view(name: &str, plan: LogicalPlan) -> LogicalPlan {
    let Some(mode) = view_mode(name) else {
        return plan;
    };
    if !holds(&plan, |_| true) {
        return plan;
    }
    swapped(&plan, |provider| rebound_clone(provider, mode)).unwrap_or(plan)
}

pub(super) fn referenced(plan: Cow<'_, LogicalPlan>) -> Cow<'_, LogicalPlan> {
    if !holds(&plan, viewed) {
        return plan;
    }
    swapped(&plan, referenced_clone).map_or(plan, Cow::Owned)
}

pub(super) fn frame_table(view: Arc<dyn TableProvider>) -> Arc<dyn TableProvider> {
    let Some(plan) = view
        .get_logical_plan()
        .filter(|plan| holds(plan, viewed))
        .map(Cow::into_owned)
    else {
        return view;
    };
    Arc::new(HeldView { plan, view })
}

fn viewed(bound: &Binding) -> bool {
    bound.mode() == BindMode::View
}

fn swapped(
    plan: &LogicalPlan,
    swap: impl Fn(&Arc<dyn TableProvider>) -> Option<Arc<dyn TableProvider>>,
) -> Option<LogicalPlan> {
    plan.clone()
        .transform_up_with_subqueries(|node| {
            Ok(match node {
                LogicalPlan::TableScan(mut scan) => {
                    match source_as_provider(&scan.source)
                        .ok()
                        .and_then(|provider| swap(&provider))
                    {
                        Some(clone) => {
                            scan.source = provider_as_source(clone);
                            Transformed::yes(LogicalPlan::TableScan(scan))
                        }
                        None => Transformed::no(LogicalPlan::TableScan(scan)),
                    }
                }
                other => Transformed::no(other),
            })
        })
        .ok()
        .filter(|rewritten| rewritten.transformed)
        .map(|rewritten| rewritten.data)
}

fn holds(plan: &LogicalPlan, wanted: impl Fn(&Binding) -> bool) -> bool {
    let mut found = false;
    let walked = plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
        {
            found = binding(&provider).is_some_and(|bound| wanted(&bound));
        }
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    });
    walked.is_ok() && found
}

pub(super) fn rebind(
    plan: LogicalPlan,
    memo: Option<&ReaderParseMemo>,
) -> Result<Transformed<LogicalPlan>> {
    let mut decided: HashMap<String, Decided> = HashMap::new();
    let mut rebound: HashMap<usize, Arc<dyn TableProvider>> = HashMap::new();
    plan.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
            && let Some(bound) = binding(&provider)
        {
            let key = bound.memo_key();
            let state = decided
                .entry(key)
                .or_insert_with_key(|key| decide(&bound, key, memo))
                .clone();
            if let Some(wrapper) = step(&provider, &bound, state)? {
                rebound.insert(scan_key(&provider), wrapper);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if rebound.is_empty() {
        return Ok(Transformed::no(plan));
    }
    plan.transform_up_with_subqueries(|node| {
        Ok(match node {
            LogicalPlan::TableScan(mut scan) => match source_as_provider(&scan.source)
                .ok()
                .and_then(|provider| rebound.get(&scan_key(&provider)))
            {
                Some(wrapper) => {
                    scan.source = provider_as_source(Arc::clone(wrapper));
                    Transformed::yes(LogicalPlan::TableScan(scan))
                }
                None => Transformed::no(LogicalPlan::TableScan(scan)),
            },
            other => Transformed::no(other),
        })
    })
}

fn scan_key(provider: &Arc<dyn TableProvider>) -> usize {
    Arc::as_ptr(provider).cast::<()>() as usize
}

fn decide(bound: &Binding, key: &str, memo: Option<&ReaderParseMemo>) -> Decided {
    if let Some(held) = memo.and_then(|memo| memo.held_table(key)) {
        return Decided::Live(held);
    }
    match bound.current() {
        Some(TableState::Live(table)) => {
            if let Some(memo) = memo {
                memo.hold_table(key, table.clone());
            }
            Decided::Live(table)
        }
        Some(TableState::Dropped) => Decided::Dropped,
        None => Decided::Unknown,
    }
}

fn step(
    provider: &Arc<dyn TableProvider>,
    bound: &Binding,
    state: Decided,
) -> Result<Option<Arc<dyn TableProvider>>> {
    let table = match state {
        Decided::Unknown => return Ok(None),
        Decided::Dropped => return Err(not_found(bound)),
        Decided::Live(table) => table,
    };
    let held = bound.table();
    if same_state(held, table.table()) {
        return Ok(None);
    }
    if bound.mode() != BindMode::View && table.table().metadata().uuid() != held.metadata().uuid() {
        return Err(table_id_mismatch(bound, held, table.table()));
    }
    let rebound =
        rebound_provider(&table).map_err(|error| DataFusionError::External(Box::new(error)))?;
    let changes = column_changes(&provider.schema(), &rebound.schema(), bound.mode());
    if !changes.is_empty() {
        return Err(columns_mismatch(bound, &changes));
    }
    Ok(Some(Arc::new(HeldIcebergScan {
        held: Arc::clone(provider),
        rebound: Arc::new(rebound),
    })))
}

fn column_changes(held: &Schema, current: &Schema, mode: BindMode) -> Vec<String> {
    let mut changes = Vec::new();
    for field in held.fields() {
        match current.field_with_name(field.name()) {
            Err(_) => changes.push(format!("`{}` has been removed", field.name())),
            Ok(now) if now.data_type() != field.data_type() => changes.push(format!(
                "`{}` type has changed from {} to {}",
                field.name(),
                field.data_type(),
                now.data_type()
            )),
            Ok(now) if now.is_nullable() != field.is_nullable() => changes.push(format!(
                "`{}` nullability has changed from {} to {}",
                field.name(),
                field.is_nullable(),
                now.is_nullable()
            )),
            Ok(_) => {}
        }
    }
    if mode == BindMode::Command {
        for field in current.fields() {
            if held.field_with_name(field.name()).is_err() {
                changes.push(format!("`{}` has been added", field.name()));
            }
        }
    }
    changes
}

fn display_name(bound: &Binding) -> String {
    let ident = bound.ident();
    std::iter::once(bound.catalog())
        .chain(ident.namespace().iter().map(String::as_str))
        .chain(std::iter::once(ident.name()))
        .map(|part| format!("`{part}`"))
        .collect::<Vec<_>>()
        .join(".")
}

fn not_found(bound: &Binding) -> DataFusionError {
    let ident = bound.ident();
    let mut parts = vec![bound.catalog()];
    parts.extend(ident.namespace().iter().map(String::as_str));
    parts.push(ident.name());
    crate::catalog_ops::table_or_view_not_found_parts(&parts)
}

fn table_id_mismatch(bound: &Binding, held: &Table, table: &Table) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH] Detected incompatible \
         changes to table {} after DataFrame/Dataset has been resolved and analyzed, meaning the \
         underlying plan is out of sync. Please, re-create DataFrame/Dataset before attempting to \
         execute the query again. Table ID has changed from {} to {}. SQLSTATE: 51024",
        display_name(bound),
        held.metadata().uuid(),
        table.metadata().uuid()
    ))
}

fn columns_mismatch(bound: &Binding, changes: &[String]) -> DataFusionError {
    let errors = changes.join("; ");
    if bound.mode() == BindMode::View {
        return DataFusionError::Plan(format!(
            "[INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION] View plan references \
             table {} whose data columns changed since the view plan was initially captured. \
             Column changes: {errors}. This indicates the table has evolved and the view based \
             on the plan must be recreated. SQLSTATE: 51024",
            display_name(bound)
        ));
    }
    DataFusionError::Plan(format!(
        "[INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH] Detected incompatible \
         changes to table {} after DataFrame/Dataset has been resolved and analyzed, meaning the \
         underlying plan is out of sync. Please, re-create DataFrame/Dataset before attempting to \
         execute the query again. Data columns have changed: {errors}. SQLSTATE: 51024",
        display_name(bound)
    ))
}

#[derive(Debug)]
struct HeldIcebergScan {
    held: Arc<dyn TableProvider>,
    rebound: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for HeldIcebergScan {
    fn schema(&self) -> SchemaRef {
        self.held.schema()
    }

    fn table_type(&self) -> TableType {
        self.held.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.rebound.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        self.rebound.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let held = self.held.schema();
        let wanted: Vec<usize> =
            projection.map_or_else(|| (0..held.fields().len()).collect(), Clone::clone);
        let current = self.rebound.schema();
        let mapped = wanted
            .iter()
            .map(|index| {
                let field = held
                    .fields()
                    .get(*index)
                    .ok_or_else(|| internal_datafusion_err!("held scan column {index} is gone"))?;
                Ok(current.index_of(field.name())?)
            })
            .collect::<Result<Vec<usize>>>()?;
        let plan = self
            .rebound
            .scan(state, Some(&mapped), filters, limit)
            .await?;
        let expected = Arc::new(held.project(&wanted)?);
        if plan.schema() == expected {
            return Ok(plan);
        }
        Ok(conformed(plan, expected))
    }
}

#[derive(Debug)]
struct HeldView {
    plan: LogicalPlan,
    view: Arc<dyn TableProvider>,
}

#[async_trait]
impl TableProvider for HeldView {
    fn schema(&self) -> SchemaRef {
        self.view.schema()
    }

    fn table_type(&self) -> TableType {
        self.view.table_type()
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        Some(referenced(Cow::Borrowed(&self.plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        self.view.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.view.scan(state, projection, filters, limit).await
    }
}

#[cfg(test)]
mod tests;
