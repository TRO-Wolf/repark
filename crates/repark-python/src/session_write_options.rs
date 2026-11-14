use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::MemTable;
use datafusion::datasource::default_table_source::source_as_provider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{DmlStatement, Expr, LogicalPlan, TableSource, TableType, WriteOp};
use datafusion::sql::parser::Statement as ParsedStatement;
use datafusion::sql::sqlparser::ast::{Expr as SqlExpr, SelectItem, SetExpr, Statement};
use pyo3::prelude::*;
use repark_core::ReparkSession;

use crate::dataframe::PyDataFrame;
use crate::deep_stack::{
    block_on_grown_if, block_on_grown_sized, frame_drive_segment_cached, sql_drive_grown,
};
use crate::fence::fenced_span;
use crate::session::PyReparkSession;

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, query, options, force_static_overwrite = false, force_dynamic_overwrite = false, source_by_name = false))]
pub fn session_sql_with_write_options(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    query: &str,
    options: HashMap<String, String>,
    force_static_overwrite: bool,
    force_dynamic_overwrite: bool,
    source_by_name: bool,
) -> PyResult<PyDataFrame> {
    let overwrite_intent = match (force_static_overwrite, force_dynamic_overwrite) {
        (false, false) => repark_core::OverwriteIntent::Session,
        (true, false) => repark_core::OverwriteIntent::Static,
        (false, true) => repark_core::OverwriteIntent::Dynamic,
        (true, true) => {
            return Err(pyo3::exceptions::PyValueError::new_err(
                crate::exceptions::mask_user_visible(
                    "force_static_overwrite and force_dynamic_overwrite are mutually exclusive",
                ),
            ));
        }
    };
    fenced_span!("py.sql", "session_sql_with_write_options", {
        let grown = sql_drive_grown(query)
            || session.deep_view_levels() > crate::deep_stack::DEEP_NESTING_DEPTH;
        if grown {
            crate::deep_stack::grown_sync(crate::deep_stack::GROWN_STACK_SEGMENT_BYTES, || {
                repark_spark::refuse_declared_function_in_sql(query)
            })
            .map_err(crate::datafusion_to_py_err)?;
        } else {
            repark_spark::refuse_declared_function_in_sql(query)
                .map_err(crate::datafusion_to_py_err)?;
        }
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let df = py
            .detach(|| {
                block_on_grown_if(
                    &runtime,
                    inner.sql_built_with_write_options(
                        query,
                        &options,
                        overwrite_intent,
                        source_by_name,
                    ),
                    grown,
                )
            })
            .map_err(crate::to_py_err)?;
        if let Some(message) =
            pending_write_refusal(&inner, query, df.logical_plan(), force_dynamic_overwrite)
        {
            return Err(crate::to_py_err(repark_core::Error::NotImplemented(
                message,
            )));
        }
        Ok(PyDataFrame::new(df, runtime))
    })
}

const PENDING_WRITE_CARD: &str = "NATIVE-DATAFRAME-WRITE-SILENT-1";

const STAGING_PREFIX: &str = "repark-staging-";

const STAGING_HEX_LEN: usize = 32;

pub(crate) fn pending_write_refusal(
    session: &ReparkSession,
    query: &str,
    plan: &LogicalPlan,
    dynamic_overwrite: bool,
) -> Option<String> {
    match plan {
        LogicalPlan::Dml(dml) => match dml.op {
            WriteOp::Insert(op) => Some(insert_refusal(session, query, dml, op, dynamic_overwrite)),
            _ => None,
        },
        LogicalPlan::Copy(copy) => Some(path_refusal(&copy_destination(&copy.output_url))),
        _ => None,
    }
}

fn insert_refusal(
    session: &ReparkSession,
    query: &str,
    dml: &DmlStatement,
    op: InsertOp,
    dynamic_overwrite: bool,
) -> String {
    let target = dml.table_name.to_quoted_string();
    let state = session.context().state();
    if let Some(reason) = unwritable(session, &state, dml) {
        return format!(
            "DataFrame.write and writeTo cannot write a repark.sql() frame into {target}: \
             {reason}, so nothing was written ({PENDING_WRITE_CARD})"
        );
    }
    let route = TableRoute {
        by_name: names_its_columns(&state, query),
        pairs: insert_pairs(&dml.input),
        frame: frame_columns(&dml.input),
        state,
        target: &target,
    };
    let workaround = match op {
        InsertOp::Append => collected(&route.insert("INSERT INTO")),
        InsertOp::Overwrite | InsertOp::Replace if in_memory(&dml.target) => format!(
            "{target} is an in-memory table, where INSERT OVERWRITE is not implemented: {}, \
             which replaces the table with the query's columns and rows",
            collected(&route.replace())
        ),
        InsertOp::Overwrite | InsertOp::Replace if dynamic_overwrite => format!(
            "on an unpartitioned table, which is every table repark.sql(...) can create, \
             overwritePartitions replaces every row: {}",
            collected(&route.insert("INSERT OVERWRITE"))
        ),
        InsertOp::Overwrite | InsertOp::Replace => collected(&route.insert("INSERT OVERWRITE")),
    };
    format!(
        "DataFrame.write and writeTo cannot write a repark.sql() frame into the table {target}: \
         the write would return without storing any rows, so nothing was written; {workaround} \
         ({PENDING_WRITE_CARD})"
    )
}

fn in_memory(target: &Arc<dyn TableSource>) -> bool {
    source_as_provider(target).is_ok_and(|provider| provider.downcast_ref::<MemTable>().is_some())
}

fn unwritable(session: &ReparkSession, state: &SessionState, dml: &DmlStatement) -> Option<String> {
    if dml.target.table_type() == TableType::View {
        return Some("it is a view, and the native door cannot write through a view".to_string());
    }
    let options = state.config_options();
    let catalog = dml
        .table_name
        .catalog()
        .unwrap_or(&options.catalog.default_catalog);
    session
        .catalogs_snapshot()
        .is_database_source(catalog)
        .then(|| {
            format!(
                "it is a table of the database source {catalog}, and the native door cannot \
                 write into a database source"
            )
        })
}

fn names_its_columns(state: &SessionState, query: &str) -> bool {
    let dialect = state.config_options().sql_parser.dialect;
    let Ok(ParsedStatement::Statement(statement)) = state.sql_to_statement(query, &dialect) else {
        return false;
    };
    matches!(statement.as_ref(), Statement::Insert(insert) if !insert.columns.is_empty())
}

fn insert_pairs(input: &LogicalPlan) -> Vec<(String, Option<String>)> {
    let LogicalPlan::Projection(projection) = input else {
        return Vec::new();
    };
    projection
        .expr
        .iter()
        .zip(projection.schema.fields())
        .map(|(expr, field)| (field.name().clone(), source_column(expr)))
        .collect()
}

fn source_column(expr: &Expr) -> Option<String> {
    let mut current = expr;
    loop {
        current = match current {
            Expr::Alias(alias) => &alias.expr,
            Expr::Cast(cast) => &cast.expr,
            Expr::TryCast(cast) => &cast.expr,
            Expr::Column(column) => return Some(column.name.clone()),
            _ => return None,
        };
    }
}

fn frame_columns(input: &LogicalPlan) -> Vec<String> {
    let mut columns = Vec::new();
    let walked = input.apply(|node| {
        let (LogicalPlan::SubqueryAlias(_) | LogicalPlan::TableScan(_)) = node else {
            return Ok(TreeNodeRecursion::Continue);
        };
        columns = node
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        Ok(TreeNodeRecursion::Stop)
    });
    if walked.is_ok() { columns } else { Vec::new() }
}

struct TableRoute<'a> {
    state: SessionState,
    target: &'a str,
    by_name: bool,
    pairs: Vec<(String, Option<String>)>,
    frame: Vec<String>,
}

fn collected(statement: &str) -> String {
    format!(
        "register the frame as a temporary view <view> and run {statement} through \
         repark.sql(...) and collect the result"
    )
}

impl TableRoute<'_> {
    fn insert(&self, verb: &str) -> String {
        let target = self.target;
        let mut listed: Vec<(&str, &str)> = self
            .pairs
            .iter()
            .filter_map(|(column, source)| {
                source.as_deref().map(|source| (column.as_str(), source))
            })
            .collect();
        if !self.by_name || listed.is_empty() {
            return format!("{verb} {target} SELECT * FROM <view>");
        }
        listed.sort_by_key(|(_, source)| {
            self.frame
                .iter()
                .position(|name| name == source)
                .unwrap_or(usize::MAX)
        });
        let columns = self.idents(listed.iter().map(|(column, _)| *column));
        let sources = self.idents(listed.iter().map(|(_, source)| *source));
        format!("{verb} {target} ({columns}) SELECT {sources} FROM <view>")
    }

    fn replace(&self) -> String {
        let target = self.target;
        if self.pairs.is_empty() {
            return format!("CREATE OR REPLACE TABLE {target} AS SELECT * FROM <view>");
        }
        let items: Vec<String> = self
            .pairs
            .iter()
            .map(|(column, source)| match source {
                Some(source) if source == column => self.ident(column),
                Some(source) => format!("{} AS {}", self.ident(source), self.ident(column)),
                None => format!("NULL AS {}", self.ident(column)),
            })
            .collect();
        format!(
            "CREATE OR REPLACE TABLE {target} AS SELECT {} FROM <view>",
            items.join(", ")
        )
    }

    fn idents<'n>(&self, names: impl Iterator<Item = &'n str>) -> String {
        names
            .map(|name| self.ident(name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn ident(&self, name: &str) -> String {
        if plain_name(name) && self.reads_as_column(name) {
            return name.to_string();
        }
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn reads_as_column(&self, name: &str) -> bool {
        let dialect = self.state.config_options().sql_parser.dialect;
        let probe = format!("SELECT {name} FROM frame_rows");
        let Ok(ParsedStatement::Statement(statement)) =
            self.state.sql_to_statement(&probe, &dialect)
        else {
            return false;
        };
        let Statement::Query(query) = statement.as_ref() else {
            return false;
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            return false;
        };
        matches!(
            select.projection.as_slice(),
            [SelectItem::UnnamedExpr(SqlExpr::Identifier(ident))] if ident.value == name
        )
    }
}

fn plain_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_lowercase() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn path_refusal(path: &str) -> String {
    format!(
        "DataFrame.write cannot write a repark.sql() frame to the path {path}: the write would \
         return without storing any rows, so nothing was written; convert the frame on a \
         ReparkSession and write the result: spark.createDataFrame(frame.collect(), \
         frame.schema), which holds every row in memory ({PENDING_WRITE_CARD})"
    )
}

pub(crate) fn copy_destination(url: &str) -> String {
    let trimmed = url.strip_suffix('/').unwrap_or(url);
    let (parent, segment) = match trimmed.rsplit_once('/') {
        Some((parent, segment)) => (Some(parent), segment),
        None => (None, trimmed),
    };
    match (staged_name(segment), parent) {
        (Some(""), Some("")) => "/".to_string(),
        (Some(""), Some(parent)) => parent.to_string(),
        (Some(""), None) => ".".to_string(),
        (Some(name), Some(parent)) => format!("{parent}/{name}"),
        (Some(name), None) => name.to_string(),
        (None, _) => url.to_string(),
    }
}

fn staged_name(segment: &str) -> Option<&str> {
    let rest = segment.strip_prefix(STAGING_PREFIX)?;
    let hex = rest.get(..STAGING_HEX_LEN)?;
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let tail = rest.get(STAGING_HEX_LEN..)?;
    if tail.is_empty() {
        return Some("");
    }
    tail.strip_prefix('-').filter(|name| !name.is_empty())
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (session, frame, url, format, mode, options, partition_by))]
pub fn session_write_path(
    py: Python<'_>,
    session: PyRef<'_, PyReparkSession>,
    frame: &PyDataFrame,
    url: &str,
    format: &str,
    mode: &str,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
) -> PyResult<usize> {
    fenced_span!("py.write", "session_write_path", {
        let runtime = Arc::clone(&session.runtime);
        let inner = session.session.clone();
        let segment = frame_drive_segment_cached(&frame.depths())?;
        let frame = crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths());
        py.detach(|| {
            block_on_grown_sized(
                &runtime,
                inner.write_path(&frame, url, format, mode, &options, &partition_by),
                segment,
            )
        })
        .map_err(crate::to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (sql, keep_verbatim))]
pub fn built_sql_user_fragment(sql: &str, keep_verbatim: bool) -> PyResult<String> {
    fenced_span!("py.sql", "built_sql_user_fragment", {
        repark_spark::spark_literals::built_fragment::canonicalize_fragment_for_default_parse(
            sql,
            keep_verbatim,
        )
        .map(std::borrow::Cow::into_owned)
        .map_err(crate::datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (session, frame, view, options, partition_by, stored_as))]
pub fn session_text_write_copy_parts(
    session: PyRef<'_, PyReparkSession>,
    frame: &PyDataFrame,
    view: &str,
    options: HashMap<String, String>,
    partition_by: Vec<String>,
    stored_as: &str,
) -> PyResult<(String, String, String)> {
    fenced_span!("py.write", "session_text_write_copy_parts", {
        session
            .session
            .text_write_copy_parts(frame.inner(), view, &options, &partition_by, stored_as)
            .map(|parts| (parts.select_sql, parts.stored_as, parts.spec_options_sql))
            .map_err(crate::to_py_err)
    })
}

#[pyfunction]
fn write_doors() -> Vec<(String, bool)> {
    repark_spark::write_door::WriteDoor::ALL
        .iter()
        .map(|door| (door.id().to_string(), door.is_spark_door()))
        .collect()
}

#[pyfunction]
#[pyo3(signature = (sql, options = None))]
fn write_door_of(sql: &str, options: Option<HashMap<String, String>>) -> PyResult<Option<String>> {
    let pairs = options.unwrap_or_default().into_iter().collect();
    repark_spark::write_door::write_door_of_sql(sql, pairs)
        .map(|door| door.map(|door| door.id().to_string()))
        .map_err(crate::datafusion_to_py_err)
}

#[pyfunction]
fn last_write_doors(session: &PyReparkSession) -> Vec<String> {
    repark_spark::write_door::last_write_doors(session.session.context())
        .into_iter()
        .map(|door| door.id().to_string())
        .collect()
}

#[cfg(test)]
#[path = "session_write_options_tests.rs"]
mod tests;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(session_sql_with_write_options, module)?)?;
    module.add_function(wrap_pyfunction!(write_doors, module)?)?;
    module.add_function(wrap_pyfunction!(write_door_of, module)?)?;
    module.add_function(wrap_pyfunction!(last_write_doors, module)?)?;
    module.add_function(wrap_pyfunction!(session_write_path, module)?)?;
    module.add_function(wrap_pyfunction!(built_sql_user_fragment, module)?)?;
    module.add_function(wrap_pyfunction!(session_text_write_copy_parts, module)?)?;
    Ok(())
}
