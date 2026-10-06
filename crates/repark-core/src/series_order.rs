#[cfg(test)]
mod tests;

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::common::{Column, DFSchema};
use datafusion::datasource::MemTable;
use datafusion::datasource::default_table_source::source_as_provider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr, TableScan};
use datafusion::prelude::SessionConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeriesOrderSource {
    Declared,
    FirstTemporal(String),
    CurrentRowOrder,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SeriesOrder {
    pub keys: Vec<SortExpr>,
    pub source: SeriesOrderSource,
}

#[must_use]
pub fn resolve_series_order(plan: &LogicalPlan, schema: &DFSchema) -> SeriesOrder {
    if let Some(keys) = declared_order(plan) {
        return SeriesOrder {
            keys,
            source: SeriesOrderSource::Declared,
        };
    }
    if let Some(key) = first_temporal_key(schema) {
        let name = match &key.expr {
            Expr::Column(column) => column.name.clone(),
            other => other.schema_name().to_string(),
        };
        return SeriesOrder {
            keys: vec![key],
            source: SeriesOrderSource::FirstTemporal(name),
        };
    }
    SeriesOrder {
        keys: Vec::new(),
        source: SeriesOrderSource::CurrentRowOrder,
    }
}

#[must_use]
pub fn declared_order(plan: &LogicalPlan) -> Option<Vec<SortExpr>> {
    let positions = declared_positions(plan)?;
    let schema = plan.schema();
    let keys = positions
        .into_iter()
        .map(|key| {
            let (qualifier, field) = schema.qualified_field(key.index);
            SortExpr::new(
                Expr::Column(Column::new(qualifier.cloned(), field.name())),
                key.asc,
                key.nulls_first,
            )
        })
        .collect();
    Some(keys)
}

#[must_use]
pub fn first_temporal_key(schema: &DFSchema) -> Option<SortExpr> {
    let fields = || schema.iter();
    let timestamp =
        fields().find(|(_, field)| matches!(field.data_type(), DataType::Timestamp(..)));
    let chosen = timestamp.or_else(|| {
        fields().find(|(_, field)| matches!(field.data_type(), DataType::Date32 | DataType::Date64))
    })?;
    let (qualifier, field) = chosen;
    Some(SortExpr::new(
        Expr::Column(Column::new(qualifier.cloned(), field.name())),
        true,
        true,
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KeyPosition {
    index: usize,
    asc: bool,
    nulls_first: bool,
}

fn declared_positions(plan: &LogicalPlan) -> Option<Vec<KeyPosition>> {
    match plan {
        LogicalPlan::Sort(sort) => {
            let schema = sort.input.schema();
            sort.expr
                .iter()
                .map(|key| {
                    Some(KeyPosition {
                        index: schema.index_of_column(plain_column(&key.expr)?).ok()?,
                        asc: key.asc,
                        nulls_first: key.nulls_first,
                    })
                })
                .collect::<Option<Vec<_>>>()
                .filter(|keys| !keys.is_empty())
        }
        LogicalPlan::Projection(projection) => {
            let below = declared_positions(&projection.input)?;
            let input = projection.input.schema();
            below
                .into_iter()
                .map(|key| {
                    let index = projection.expr.iter().position(|expr| {
                        plain_column(expr).is_some_and(|column| {
                            input.index_of_column(column).ok() == Some(key.index)
                        })
                    })?;
                    Some(KeyPosition { index, ..key })
                })
                .collect()
        }
        LogicalPlan::Filter(filter) => declared_positions(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => declared_positions(&alias.input),
        LogicalPlan::Window(window) => declared_positions(&window.input),
        LogicalPlan::Limit(limit) => declared_positions(&limit.input),
        LogicalPlan::TableScan(scan) => memtable_positions(scan),
        _ => None,
    }
}

fn plain_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => plain_column(&alias.expr),
        _ => None,
    }
}

fn memtable_positions(scan: &TableScan) -> Option<Vec<KeyPosition>> {
    let provider = source_as_provider(&scan.source).ok()?;
    let provider_any: &dyn Any = provider.as_ref();
    let table = provider_any.downcast_ref::<MemTable>()?;
    let ordering = table.sort_order.lock().first().cloned()?;
    let table_schema = provider.schema();
    ordering
        .iter()
        .map(|key| {
            let Expr::Column(column) = &key.expr else {
                return None;
            };
            let table_index = table_schema.index_of(&column.name).ok()?;
            let index = match &scan.projection {
                Some(projection) => projection.iter().position(|&kept| kept == table_index)?,
                None => table_index,
            };
            Some(KeyPosition {
                index,
                asc: key.asc,
                nulls_first: key.nulls_first,
            })
        })
        .collect::<Option<Vec<_>>>()
        .filter(|keys| !keys.is_empty())
}

#[derive(Debug, Clone)]
pub struct SeriesOrderNotice {
    issued: Arc<AtomicBool>,
}

impl Default for SeriesOrderNotice {
    fn default() -> Self {
        Self {
            issued: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl ConfigExtension for SeriesOrderNotice {
    const PREFIX: &'static str = "repark.series";
}

impl ExtensionOptions for SeriesOrderNotice {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the series-order notice is session state",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub(crate) fn with_series_order_notice(config: SessionConfig) -> SessionConfig {
    config.with_option_extension(SeriesOrderNotice::default())
}

#[must_use]
pub fn claim_series_order_notice(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SeriesOrderNotice>()
        .is_none_or(|notice| !notice.issued.swap(true, Ordering::AcqRel))
}
