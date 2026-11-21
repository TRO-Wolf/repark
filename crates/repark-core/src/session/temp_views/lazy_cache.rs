use std::any::Any;
use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::array::RecordBatch;
use arrow::compute::{
    SortColumn, SortOptions, concat_batches, lexsort_to_indices, take_record_batch,
};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, Statistics};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::source_as_provider;
use datafusion::error::{DataFusionError, Result as FusionResult};
use datafusion::execution::session_state::SessionState;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, SortExpr, TableType};
use datafusion::physical_expr::{LexOrdering, create_physical_sort_exprs};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use datafusion::prelude::DataFrame;
use futures::TryStreamExt;
use iceberg::table::Table;
use repark_common::{Error, Result};
use repark_iceberg::catalog::session_tables::{
    HeldTable, SessionTables, TableState, binding, same_state,
};
use tokio::sync::OnceCell;

use super::super::ReparkSession;
use crate::column_resolution::{on_grown_stack_with, run_on_grown_stack};
use crate::engine_err;
use crate::extension::{ReaderParseMemo, TableRead, reader_parse_memo};
use crate::temp_view::TempViewHome;

const LIVE_READ_WAIT: Duration = Duration::from_secs(1);
const RED_ZONE_BYTES: usize = 2 * 1024 * 1024;
const SEGMENT_BYTES: usize = 128 * 1024 * 1024;
const NODE_NAME: &str = "LazyCacheExec";

type LiveState = dyn Fn() -> Option<SessionState> + Send + Sync;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LazyStacks {
    pub(crate) segment: Option<usize>,
}

impl LazyStacks {
    fn sizes(self) -> (usize, usize) {
        self.segment
            .map_or((RED_ZONE_BYTES, SEGMENT_BYTES), |bytes| (bytes, bytes))
    }

    fn run<T>(self, work: impl FnOnce() -> T) -> T {
        let (red, segment) = self.sizes();
        run_on_grown_stack(red, segment, work)
    }
}

struct GrownPlan {
    plan: LogicalPlan,
    stacks: LazyStacks,
}

impl GrownPlan {
    fn new(plan: LogicalPlan, stacks: LazyStacks) -> Self {
        Self { plan, stacks }
    }
}

impl std::ops::Deref for GrownPlan {
    type Target = LogicalPlan;

    fn deref(&self) -> &LogicalPlan {
        &self.plan
    }
}

impl Drop for GrownPlan {
    fn drop(&mut self) {
        let empty = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let plan = std::mem::replace(&mut self.plan, empty);
        self.stacks.run(|| drop(plan));
    }
}

struct Within<F> {
    memo: Option<Arc<ReaderParseMemo>>,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Within<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<F::Output> {
        let memo = self.memo.clone();
        let inner = self.inner.as_mut();
        ReaderParseMemo::within(memo, || inner.poll(context))
    }
}

fn within<F: Future>(memo: Option<Arc<ReaderParseMemo>>, future: F) -> Within<F> {
    Within {
        memo,
        inner: Box::pin(future),
    }
}

struct Dep {
    read: TableRead,
    epoch: Option<u64>,
    seen: Option<HeldTable>,
}

impl Dep {
    fn holds(&mut self, tables: &SessionTables) -> bool {
        let Some(epoch) = self.epoch else {
            return false;
        };
        let now = tables.local_epoch(&self.read.catalog, &self.read.ident);
        if now.changed == epoch {
            return true;
        }
        if now.refreshed > epoch {
            return false;
        }
        let Some(TableState::Live(current)) = tables.current(&self.read.catalog, &self.read.ident)
        else {
            return false;
        };
        let kept = self.seen.as_ref().is_some_and(|seen| {
            !same_state(seen.table(), current.table()) && same_data(seen.table(), current.table())
        });
        if kept {
            self.epoch = Some(now.changed);
            self.seen = Some(current);
        }
        kept
    }
}

fn same_data(seen: &Table, current: &Table) -> bool {
    let (seen, current) = (seen.metadata(), current.metadata());
    seen.uuid() == current.uuid()
        && seen.current_snapshot_id() == current.current_snapshot_id()
        && seen.history().last() == current.history().last()
}

fn live(state: Option<TableState>) -> Option<HeldTable> {
    match state {
        Some(TableState::Live(table)) => Some(table),
        _ => None,
    }
}

struct Fill {
    batches: Vec<RecordBatch>,
    memo: Arc<ReaderParseMemo>,
    deps: Mutex<Vec<Dep>>,
}

enum Mode {
    Armed(Arc<OnceCell<Arc<Fill>>>),
    Released,
}

struct Decided {
    state: SessionState,
    analyzed: GrownPlan,
    optimized: GrownPlan,
}

enum Step {
    Serve(Arc<Fill>),
    Fill(Arc<OnceCell<Arc<Fill>>>, Decided, Arc<ReaderParseMemo>),
    Pass(Decided),
}

struct Shared {
    plan: GrownPlan,
    options: Arc<ConfigOptions>,
    live: Arc<LiveState>,
    home: Weak<TempViewHome>,
    schema: SchemaRef,
    order: Vec<SortExpr>,
    budgets: Mutex<(Option<u64>, Option<u64>)>,
    stacks: LazyStacks,
    owner: Option<Weak<ReaderParseMemo>>,
    mode: Mutex<Mode>,
    tables: Option<Arc<SessionTables>>,
    reads: Vec<TableRead>,
}

pub(crate) struct LazyCacheTable {
    shared: Arc<Shared>,
}

impl fmt::Debug for LazyCacheTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazyCacheTable")
            .field("filled", &self.filled())
            .field("released", &self.released())
            .finish_non_exhaustive()
    }
}

impl LazyCacheTable {
    pub(super) async fn try_new(
        session: &ReparkSession,
        frame: DataFrame,
        budgets: (Option<u64>, Option<u64>),
        stacks: LazyStacks,
        owner: Option<Arc<ReaderParseMemo>>,
    ) -> Result<Self> {
        let (state, plan) = frame.into_parts();
        let options = Arc::clone(state.config_options());
        let analyzed = stacks.run(|| {
            state
                .analyzer()
                .execute_and_check(plan.clone(), state.config_options(), |_, _| {})
        });
        let analyzed = GrownPlan::new(analyzed.map_err(engine_err)?, stacks);
        let schema = stored_schema(&analyzed)?;
        let tables = state.config().get_extension::<SessionTables>();
        let reads = stacks.run(|| pinned_reads(&analyzed, owner.as_deref()));
        let order = declared_order(&state, &analyzed, &schema, stacks).await;
        let shared = session.context().state_ref();
        let weak = Arc::downgrade(&shared);
        let live: Arc<LiveState> = Arc::new(move || {
            let state = weak.upgrade()?;
            state
                .try_read_recursive_for(LIVE_READ_WAIT)
                .map(|guard| guard.clone())
        });
        Ok(Self {
            shared: Arc::new(Shared {
                plan: GrownPlan::new(plan, stacks),
                options,
                live,
                home: Arc::downgrade(&session.temp_view_home),
                schema,
                order,
                budgets: Mutex::new(budgets),
                stacks,
                owner: owner
                    .filter(|memo| memo.is_settled())
                    .map(|memo| Arc::downgrade(&memo)),
                mode: Mutex::new(Mode::Armed(Arc::new(OnceCell::new()))),
                tables,
                reads,
            }),
        })
    }

    pub(crate) fn release(&self) {
        *self
            .shared
            .mode
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Mode::Released;
    }

    pub(crate) fn set_budgets(&self, budgets: (Option<u64>, Option<u64>)) {
        *self
            .shared
            .budgets
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = budgets;
    }

    #[must_use]
    pub(crate) fn filled(&self) -> bool {
        self.shared.fill().is_some()
    }

    #[must_use]
    pub(crate) fn released(&self) -> bool {
        matches!(
            *self
                .shared
                .mode
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            Mode::Released
        )
    }

    #[must_use]
    pub(crate) fn lineage_options(&self) -> Arc<ConfigOptions> {
        Arc::clone(&self.shared.options)
    }

    pub(crate) fn retained_batches(&self) -> Vec<RecordBatch> {
        self.shared
            .fill()
            .map(|fill| fill.batches.clone())
            .unwrap_or_default()
    }
}

#[must_use]
pub(crate) fn lazy_cache_of(provider: &dyn TableProvider) -> Option<&LazyCacheTable> {
    let any: &dyn Any = provider;
    any.downcast_ref::<LazyCacheTable>()
}

fn pinned_reads(analyzed: &LogicalPlan, owner: Option<&ReaderParseMemo>) -> Vec<TableRead> {
    let mut reads: Vec<TableRead> = Vec::new();
    let walked = analyzed.apply_with_subqueries(|node| {
        if let LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = source_as_provider(&scan.source)
            && let Some(bound) = binding(&provider)
        {
            let key = bound.memo_key();
            if let Some(owner) = owner {
                owner.note_read(&key, bound.catalog(), bound.ident());
                if let Some(table) = live(bound.current()) {
                    owner.hold_table(&key, table);
                }
            }
            if reads.iter().all(|read| read.key != key) {
                reads.push(TableRead {
                    key,
                    catalog: bound.catalog().to_string(),
                    ident: bound.ident().clone(),
                });
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    if walked.is_err() {
        reads.clear();
    }
    reads
}

fn stored_schema(analyzed: &LogicalPlan) -> Result<SchemaRef> {
    let schema = Arc::new(analyzed.schema().as_arrow().clone());
    let (schema, _) =
        crate::sorted_view::apply_tighten_provenance_on_materialize(analyzed, schema, Vec::new())?;
    let (schema, _) = super::super::df_guards::attr_id::strip_record_batches(schema, Vec::new())
        .map_err(engine_err)?;
    Ok(schema)
}

async fn declared_order(
    state: &SessionState,
    analyzed: &LogicalPlan,
    schema: &SchemaRef,
    stacks: LazyStacks,
) -> Vec<SortExpr> {
    let throwaway = Some(Arc::new(ReaderParseMemo::default()));
    let optimized = stacks.run(|| {
        ReaderParseMemo::within(throwaway.clone(), || {
            state
                .optimizer()
                .optimize(analyzed.clone(), state, |_, _| {})
        })
    });
    let Ok(optimized) = optimized else {
        return Vec::new();
    };
    let optimized = GrownPlan::new(optimized, stacks);
    let (red, segment) = stacks.sizes();
    let planning = on_grown_stack_with(
        red,
        segment,
        within(
            throwaway,
            state
                .query_planner()
                .create_physical_plan(&optimized, state),
        ),
    );
    let Ok(physical) = planning.await else {
        return Vec::new();
    };
    let keys = order_keys(physical.as_ref(), schema);
    stacks.run(|| drop(physical));
    keys.unwrap_or_default()
}

fn order_keys(physical: &dyn ExecutionPlan, schema: &SchemaRef) -> Option<Vec<SortExpr>> {
    if physical.output_partitioning().partition_count() != 1 {
        return None;
    }
    let ordering = physical.properties().output_ordering()?;
    crate::sorted_view::ordered_cache_sort_exprs(ordering, physical.schema().as_ref(), schema)
}

impl Shared {
    fn fill(&self) -> Option<Arc<Fill>> {
        match &*self.mode.lock().unwrap_or_else(PoisonError::into_inner) {
            Mode::Armed(cell) => cell.get().map(Arc::clone),
            Mode::Released => None,
        }
    }

    fn owner_acts(&self, acting: Option<&Arc<ReaderParseMemo>>) -> bool {
        let owner = self.owner.as_ref().and_then(Weak::upgrade);
        matches!((owner, acting), (Some(owner), Some(acting)) if Arc::ptr_eq(&owner, acting))
    }

    fn step(&self, acting: Option<&Arc<ReaderParseMemo>>) -> FusionResult<Step> {
        let cell = match &*self.mode.lock().unwrap_or_else(PoisonError::into_inner) {
            Mode::Armed(cell) => Some(Arc::clone(cell)),
            Mode::Released => None,
        };
        let cell = match cell {
            Some(cell) if !self.owner_acts(acting) => cell,
            _ => return Ok(Step::Pass(self.decide(acting.cloned())?)),
        };
        let fill = cell.get().map(Arc::clone);
        let filled = fill.is_some();
        let fresh = fill.filter(|fill| self.fresh(fill));
        let cell = match (filled, &fresh) {
            (true, None) => match self.rearm(&cell) {
                Some(cell) => cell,
                None => return Ok(Step::Pass(self.decide(acting.cloned())?)),
            },
            _ => cell,
        };
        if acting.is_some_and(|memo| self.kept_apart(memo, fresh.as_deref())) {
            return Ok(Step::Pass(self.decide(acting.cloned())?));
        }
        if let Some(fill) = fresh {
            return Ok(Step::Serve(fill));
        }
        let memo = Arc::new(ReaderParseMemo::default());
        let decided = self.decide(Some(Arc::clone(&memo)))?;
        Ok(Step::Fill(cell, decided, memo))
    }

    fn fresh(&self, fill: &Fill) -> bool {
        let Some(tables) = self.tables.as_deref() else {
            return true;
        };
        let mut deps = fill.deps.lock().unwrap_or_else(PoisonError::into_inner);
        deps.iter_mut().all(|dep| dep.holds(tables))
    }

    fn rearm(&self, stale: &Arc<OnceCell<Arc<Fill>>>) -> Option<Arc<OnceCell<Arc<Fill>>>> {
        let mut mode = self.mode.lock().unwrap_or_else(PoisonError::into_inner);
        match &*mode {
            Mode::Armed(cell) if !Arc::ptr_eq(cell, stale) => Some(Arc::clone(cell)),
            Mode::Armed(_) => {
                let cell = Arc::new(OnceCell::new());
                *mode = Mode::Armed(Arc::clone(&cell));
                Some(cell)
            }
            Mode::Released => None,
        }
    }

    fn kept_apart(&self, acting: &ReaderParseMemo, fill: Option<&Fill>) -> bool {
        let Some(tables) = self.tables.as_deref() else {
            return false;
        };
        let deps = fill.map(|fill| fill.deps.lock().unwrap_or_else(PoisonError::into_inner));
        let served = |read: &TableRead| match deps
            .as_ref()
            .and_then(|deps| deps.iter().find(|dep| dep.read.key == read.key))
        {
            Some(dep) => dep.seen.clone(),
            None => live(tables.current(&read.catalog, &read.ident)),
        };
        let extra = deps
            .as_ref()
            .map(|deps| deps.iter().map(|dep| dep.read.clone()).collect::<Vec<_>>())
            .unwrap_or_default();
        self.reads.iter().chain(extra.iter()).any(|read| {
            acting.held_table(&read.key).is_some_and(|held| {
                served(read).is_none_or(|served| !same_data(held.table(), served.table()))
            })
        })
    }

    fn deps(&self, memo: &ReaderParseMemo) -> Vec<Dep> {
        let Some(tables) = self.tables.as_deref() else {
            return Vec::new();
        };
        let mut reads = self.reads.clone();
        for read in memo.reads() {
            if reads.iter().all(|known| known.key != read.key) {
                reads.push(read);
            }
        }
        reads
            .into_iter()
            .map(|read| {
                let now = tables.local_epoch(&read.catalog, &read.ident).changed;
                let current = live(tables.current(&read.catalog, &read.ident));
                let seen = memo.held_table(&read.key).or_else(|| current.clone());
                let valid = match (&seen, &current) {
                    (Some(seen), Some(current)) => same_data(seen.table(), current.table()),
                    (None, None) => true,
                    _ => false,
                };
                Dep {
                    read,
                    epoch: valid.then_some(now),
                    seen: if valid { current } else { seen },
                }
            })
            .collect()
    }

    fn decide(&self, memo: Option<Arc<ReaderParseMemo>>) -> FusionResult<Decided> {
        let mut state = (self.live)().ok_or_else(|| {
            DataFusionError::Execution(
                "lazy cache: the session that registered this cache is gone".to_string(),
            )
        })?;
        *state.config_mut().options_mut() = (*self.options).clone();
        state.mark_start_execution();
        self.stacks.run(|| {
            let analyzed = state.analyzer().execute_and_check(
                LogicalPlan::clone(&self.plan),
                state.config_options(),
                |_, _| {},
            )?;
            let optimized = ReaderParseMemo::within(memo, || {
                state
                    .optimizer()
                    .optimize(analyzed.clone(), &state, |_, _| {})
            })?;
            Ok(Decided {
                analyzed: GrownPlan::new(analyzed, self.stacks),
                optimized: GrownPlan::new(optimized, self.stacks),
                state,
            })
        })
    }

    async fn retained(&self) -> FusionResult<(HashSet<usize>, u64)> {
        let Some(home) = self.home.upgrade() else {
            return Ok((HashSet::new(), 0));
        };
        let Some(schema) = home.provider.as_ref() else {
            return Ok((HashSet::new(), 0));
        };
        super::super::cache_budget::buffer_set_of(schema.as_ref())
            .await
            .map_err(external)
    }

    async fn collect(
        &self,
        decided: Decided,
        memo: Option<Arc<ReaderParseMemo>>,
        budgets: (Option<u64>, Option<u64>),
    ) -> FusionResult<Vec<RecordBatch>> {
        let planning = decided
            .state
            .query_planner()
            .create_physical_plan(&decided.optimized, &decided.state);
        let physical = within(memo.clone(), planning).await?;
        let keys = order_keys(physical.as_ref(), &self.schema);
        let retained = if budgets.1.is_some() {
            self.retained().await?
        } else {
            (HashSet::new(), 0)
        };
        let task = Arc::new(TaskContext::from(&decided.state));
        let stream = datafusion::physical_plan::execute_stream(physical, task)?;
        let batches = within(memo, super::drain_budgeted(stream, budgets, retained)).await?;
        let batches = self
            .stacks
            .run(|| self.stored(&decided.analyzed, batches))?;
        if self.order.is_empty() || keys.as_deref() == Some(self.order.as_slice()) {
            Ok(batches)
        } else {
            sorted_by(&self.schema, batches, &self.order)
        }
    }

    fn stored(
        &self,
        analyzed: &LogicalPlan,
        batches: Vec<RecordBatch>,
    ) -> FusionResult<Vec<RecordBatch>> {
        let schema = Arc::new(analyzed.schema().as_arrow().clone());
        let (schema, batches) =
            crate::sorted_view::apply_tighten_provenance_on_materialize(analyzed, schema, batches)
                .map_err(external)?;
        let batches = super::conform_batches_to_schema(&schema, &batches).map_err(external)?;
        let (_, batches) = super::super::df_guards::attr_id::strip_record_batches(schema, batches)?;
        super::conform_batches_to_schema(&self.schema, &batches).map_err(external)
    }

    async fn batches(
        self: Arc<Self>,
        step: Step,
        acting: Option<Arc<ReaderParseMemo>>,
    ) -> FusionResult<Vec<RecordBatch>> {
        let fill = match step {
            Step::Pass(decided) => {
                return Box::pin(self.collect(decided, acting, (None, None))).await;
            }
            Step::Serve(fill) => fill,
            Step::Fill(cell, decided, memo) => {
                let budgets = *self.budgets.lock().unwrap_or_else(PoisonError::into_inner);
                let filling = Box::pin(self.collect(decided, Some(Arc::clone(&memo)), budgets));
                let shared = Arc::clone(&self);
                let fill = cell
                    .get_or_try_init(|| async move {
                        filling.await.map(|batches| {
                            let deps = Mutex::new(shared.deps(&memo));
                            Arc::new(Fill {
                                batches,
                                memo,
                                deps,
                            })
                        })
                    })
                    .await?;
                Arc::clone(fill)
            }
        };
        if let Some(acting) = acting {
            acting.adopt(&fill.memo);
        }
        Ok(fill.batches.clone())
    }
}

fn external(error: Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn sorted_by(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    order: &[SortExpr],
) -> FusionResult<Vec<RecordBatch>> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let whole = concat_batches(schema, &batches)?;
    let mut columns = Vec::with_capacity(order.len());
    for key in order {
        let Expr::Column(column) = &key.expr else {
            return Err(DataFusionError::Internal(
                "lazy cache: a declared sort key is not a column".to_string(),
            ));
        };
        let index = schema.index_of(column.name())?;
        columns.push(SortColumn {
            values: Arc::clone(whole.column(index)),
            options: Some(SortOptions {
                descending: !key.asc,
                nulls_first: key.nulls_first,
            }),
        });
    }
    let indices = lexsort_to_indices(&columns, None)?;
    Ok(vec![take_record_batch(&whole, &indices)?])
}

#[async_trait]
impl TableProvider for LazyCacheTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.shared.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> FusionResult<Arc<dyn ExecutionPlan>> {
        let acting = reader_parse_memo();
        let mut ordering = Vec::new();
        if !self.shared.order.is_empty() {
            let frame = DFSchema::try_from(Arc::clone(&self.shared.schema))?;
            let keys =
                create_physical_sort_exprs(&self.shared.order, &frame, state.execution_props())?;
            ordering.extend(LexOrdering::new(keys));
        }
        let template = MemorySourceConfig::try_new(
            &[Vec::new()],
            Arc::clone(&self.shared.schema),
            projection.cloned(),
        )?
        .try_with_sort_information(ordering.clone())?;
        let properties = Arc::clone(DataSourceExec::from_data_source(template).properties());
        Ok(Arc::new(LazyCacheExec {
            shared: Arc::clone(&self.shared),
            projection: projection.cloned(),
            limit,
            ordering,
            properties,
            acting,
        }))
    }
}

struct LazyCacheExec {
    shared: Arc<Shared>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    ordering: Vec<LexOrdering>,
    properties: Arc<PlanProperties>,
    acting: Option<Arc<ReaderParseMemo>>,
}

impl fmt::Debug for LazyCacheExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct(NODE_NAME).finish_non_exhaustive()
    }
}

impl DisplayAs for LazyCacheExec {
    fn fmt_as(&self, _kind: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        let state = if self.shared.fill().is_some() {
            "filled"
        } else {
            "pending"
        };
        write!(formatter, "{NODE_NAME}: {state}")
    }
}

impl ExecutionPlan for LazyCacheExec {
    fn name(&self) -> &str {
        NODE_NAME
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> FusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> FusionResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> FusionResult<SendableRecordBatchStream> {
        let step = self.shared.step(self.acting.as_ref())?;
        let shared = Arc::clone(&self.shared);
        let acting = self.acting.clone();
        let projection = self.projection.clone();
        let ordering = self.ordering.clone();
        let limit = self.limit;
        let (red, segment) = self.shared.stacks.sizes();
        let opened = on_grown_stack_with(red, segment, async move {
            let schema = Arc::clone(&shared.schema);
            let batches = Box::pin(shared.batches(step, acting)).await?;
            let source = MemorySourceConfig::try_new(&[batches], schema, projection)?
                .with_limit(limit)
                .try_with_sort_information(ordering)?;
            DataSourceExec::from_data_source(source).execute(partition, context)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(opened).try_flatten(),
        )))
    }
}

#[cfg(test)]
mod tests;
