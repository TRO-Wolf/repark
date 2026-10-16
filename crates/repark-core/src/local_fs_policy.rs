use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use url::Url;

use crate::catalog_state::{CatalogRegistry, memory_warehouse_fallback_root};
use crate::dialect::{EngineContext, SqlDialect};
use crate::pre_execute::PreExecute;

pub const ALLOW_LOCAL_FILESYSTEM_DDL_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

pub const NATIVE_ENTRY_REFUSAL: &str = "repark.sql() does not allow local-file DDL";

const REPARK_SQL_EXTENSION_PREFIX: &str = "repark.sql";

const ALLOW_LOCAL_FILESYSTEM_DDL_FIELD: &str = "allow_local_filesystem_ddl";

#[derive(Debug, Clone, Copy, Default)]
pub struct LocalFsGuardedDialect;

#[async_trait(?Send)]
impl SqlDialect for LocalFsGuardedDialect {
    async fn execute(&self, cx: EngineContext<'_>, query: &str) -> Result<DataFrame> {
        let belt = PreExecute::from_engine_context(&cx);
        let plan = belt.plan(query).await?;
        refuse_writes(cx.catalogs, &plan, Entry::NoOptIn)?;
        belt.guard(&plan)?;
        belt.execute(plan).await
    }
}

#[derive(Debug, Clone, Copy)]
enum Entry {
    WithOptIn,
    NoOptIn,
}

#[must_use]
pub fn allow_local_filesystem_ddl(ctx: &SessionContext) -> bool {
    let config = ctx.copied_config();
    config
        .options()
        .extensions
        .iter()
        .filter(|(prefix, _)| *prefix == REPARK_SQL_EXTENSION_PREFIX)
        .flat_map(|(_, extension)| extension.entries())
        .find(|entry| {
            let key = entry.key.as_str();
            let field = key
                .strip_prefix(REPARK_SQL_EXTENSION_PREFIX)
                .and_then(|rest| rest.strip_prefix('.'))
                .unwrap_or(key);
            field == ALLOW_LOCAL_FILESYSTEM_DDL_FIELD
        })
        .and_then(|entry| entry.value)
        .is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "true" | "1"))
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_local_filesystem_plan(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    if allow_local_filesystem_ddl(ctx) {
        return Ok(());
    }
    refuse_local_filesystem_writes(catalogs, plan)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_local_filesystem_writes(
    catalogs: &CatalogRegistry,
    plan: &LogicalPlan,
) -> Result<()> {
    refuse_writes(catalogs, plan, Entry::WithOptIn)
}

fn refuse_writes(catalogs: &CatalogRegistry, plan: &LogicalPlan, entry: Entry) -> Result<()> {
    plan.apply(|node| {
        match node {
            LogicalPlan::Explain(_) => return Ok(TreeNodeRecursion::Jump),
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(create)) => {
                refuse_local_path(catalogs, "CREATE EXTERNAL TABLE", &create.location, entry)?;
            }
            LogicalPlan::Copy(copy_to) => {
                refuse_local_path(catalogs, "COPY TO", &copy_to.output_url, entry)?;
            }
            _ => {}
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

fn refuse_local_path(
    catalogs: &CatalogRegistry,
    surface: &str,
    raw_location: &str,
    entry: Entry,
) -> Result<()> {
    let contained = match read_location(raw_location) {
        Reading::Remote => return Ok(()),
        Reading::Local(written) => written_path_contained(catalogs, &written),
        Reading::Unreadable => false,
    };
    if contained {
        return Ok(());
    }
    Err(DataFusionError::Plan(match entry {
        Entry::WithOptIn => format!(
            "{surface} to local filesystem path `{raw_location}` is disabled by default. Set \
             conf `{ALLOW_LOCAL_FILESYSTEM_DDL_KEY}` = true to allow local CREATE EXTERNAL \
             TABLE / COPY TO outside the session warehouse root, or use a path under a \
             registered warehouse (grandfather). Remote locations (s3://, s3a://) are \
             unaffected."
        ),
        Entry::NoOptIn => format!(
            "{surface} to local filesystem path `{raw_location}` is refused: \
             {NATIVE_ENTRY_REFUSAL} (CREATE EXTERNAL TABLE or COPY TO with a local location) \
             and this entry has no opt-in. Remote locations (s3://, s3a://) are unaffected."
        ),
    }))
}

#[must_use]
pub fn strip_ascii_prefix_ci<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let head = value.get(..prefix.len())?;
    if head.eq_ignore_ascii_case(prefix) {
        value.get(prefix.len()..)
    } else {
        None
    }
}

enum Reading {
    Remote,
    Local(PathBuf),
    Unreadable,
}

fn read_location(location: &str) -> Reading {
    let Ok(listing) = ListingTableUrl::parse(location) else {
        return Reading::Unreadable;
    };
    let url: &Url = listing.as_ref();
    if url.scheme() != "file" {
        return Reading::Remote;
    }
    if url.host_str().is_some_and(|host| !host.is_empty()) {
        return Reading::Unreadable;
    }
    let mut written = PathBuf::from("/");
    for part in listing.prefix().parts() {
        written.push(part.as_ref());
    }
    Reading::Local(written)
}

#[must_use]
pub fn local_filesystem_path(location: &str) -> Option<PathBuf> {
    match read_location(location) {
        Reading::Local(written) => Some(written),
        Reading::Remote | Reading::Unreadable => None,
    }
}

#[must_use]
pub fn location_under_any_warehouse(catalogs: &CatalogRegistry, location: &str) -> bool {
    match read_location(location) {
        Reading::Local(written) => written_path_contained(catalogs, &written),
        Reading::Remote | Reading::Unreadable => false,
    }
}

fn written_path_contained(catalogs: &CatalogRegistry, written: &Path) -> bool {
    let Some(landing) = resolve_physical_destination(written) else {
        return false;
    };
    catalogs
        .local_warehouse_roots()
        .iter()
        .filter_map(|root| warehouse_root(root))
        .any(|root| landing.starts_with(&root))
}

fn warehouse_root(root: &str) -> Option<PathBuf> {
    resolve_physical_destination(&memory_warehouse_fallback_root(root))
}

#[must_use]
pub fn resolve_physical_destination(path: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(path).ok()?;
    let components: Vec<Component<'_>> = absolute.components().collect();
    for existing in (1..=components.len()).rev() {
        let (head, tail) = components.split_at(existing);
        let ancestor: PathBuf = head.iter().collect();
        match ancestor.symlink_metadata() {
            Ok(_) => return join_missing_tail(ancestor.canonicalize().ok()?, tail),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(_) => return None,
        }
    }
    None
}

fn join_missing_tail(mut resolved: PathBuf, tail: &[Component<'_>]) -> Option<PathBuf> {
    for component in tail {
        match component {
            Component::Normal(name) => resolved.push(name),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(resolved)
}

#[cfg(test)]
mod tests;
