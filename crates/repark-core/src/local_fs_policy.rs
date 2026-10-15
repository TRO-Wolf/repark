use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use async_trait::async_trait;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionContext};
use url::Url;

use crate::catalog_state::CatalogRegistry;
use crate::dialect::{EngineContext, SqlDialect};
use crate::pre_execute::PreExecute;

pub const ALLOW_LOCAL_FILESYSTEM_DDL_KEY: &str = "repark.sql.allowLocalFilesystemDDL";

pub const NATIVE_ENTRY_REFUSAL: &str = "repark.sql() does not allow local-file DDL";

const REPARK_SQL_EXTENSION_PREFIX: &str = "repark.sql";

const ALLOW_LOCAL_FILESYSTEM_DDL_FIELD: &str = "allow_local_filesystem_ddl";

const DOT_STANDINS: [char; 3] = ['\u{E000}', '\u{E001}', '\u{E002}'];

const GLOB_START_CHARS: [char; 3] = ['?', '*', '['];

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
        Reading::Local(local) => local_location_contained(catalogs, &local),
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
    Local(LocalLocation),
    Unreadable,
}

struct LocalLocation {
    written: PathBuf,
    typed: PathBuf,
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
    let typed = if Path::new(location).is_absolute() {
        typed_filesystem_path(location)
    } else {
        match Url::parse(location) {
            Ok(_) => typed_file_url_path(location),
            Err(url::ParseError::RelativeUrlWithoutBase) => typed_filesystem_path(location),
            Err(_) => None,
        }
    };
    match typed {
        Some(typed) => Reading::Local(LocalLocation { written, typed }),
        None => Reading::Unreadable,
    }
}

fn typed_filesystem_path(location: &str) -> Option<PathBuf> {
    let listed = glob_free_prefix(location);
    let path = Path::new(listed);
    if path.is_absolute() {
        return Some(path.to_path_buf());
    }
    Some(std::env::current_dir().ok()?.join(path))
}

fn glob_free_prefix(location: &str) -> &str {
    let mut last_separator = 0;
    for (index, ch) in location.char_indices() {
        if GLOB_START_CHARS.contains(&ch) {
            return if last_separator == 0 {
                "."
            } else {
                location.get(..last_separator).unwrap_or(".")
            };
        }
        if std::path::is_separator(ch) {
            last_separator = index + ch.len_utf8();
        }
    }
    location
}

fn typed_file_url_path(location: &str) -> Option<PathBuf> {
    let standin = DOT_STANDINS
        .iter()
        .copied()
        .find(|standin| !location.contains(*standin))?;
    let shielded = location
        .replace("%2e", ".")
        .replace("%2E", ".")
        .replace('.', &standin.to_string());
    let url = Url::parse(&shielded).ok()?;
    if url.scheme() != "file" || url.host_str().is_some_and(|host| !host.is_empty()) {
        return None;
    }
    let decoded = percent_decoded(url.path())?;
    let mut typed = PathBuf::from("/");
    for segment in decoded.split('/').filter(|segment| !segment.is_empty()) {
        typed.push(segment.replace(standin, "."));
    }
    Some(typed)
}

fn percent_decoded(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while let Some(&byte) = bytes.get(index) {
        let escaped = if byte == b'%' {
            bytes.get(index + 1..index + 3).and_then(hex_pair)
        } else {
            None
        };
        if let Some(value) = escaped {
            decoded.push(value);
            index += 3;
        } else {
            decoded.push(byte);
            index += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_pair(pair: &[u8]) -> Option<u8> {
    let digit = |byte: u8| char::from(byte).to_digit(16);
    let (&high, &low) = (pair.first()?, pair.get(1)?);
    u8::try_from(digit(high)? * 16 + digit(low)?).ok()
}

#[must_use]
pub fn local_filesystem_path(location: &str) -> Option<PathBuf> {
    match read_location(location) {
        Reading::Local(local) => Some(local.written),
        Reading::Remote | Reading::Unreadable => None,
    }
}

#[must_use]
pub fn location_under_any_warehouse(catalogs: &CatalogRegistry, location: &str) -> bool {
    match read_location(location) {
        Reading::Local(local) => local_location_contained(catalogs, &local),
        Reading::Remote | Reading::Unreadable => false,
    }
}

fn local_location_contained(catalogs: &CatalogRegistry, local: &LocalLocation) -> bool {
    let (Some(written), Some(typed)) = (follow(&local.written), follow(&local.typed)) else {
        return false;
    };
    if written.resolved != typed.resolved {
        return false;
    }
    catalogs
        .local_warehouse_roots()
        .iter()
        .filter_map(|root| warehouse_root(root))
        .any(|root| {
            written.resolved.starts_with(&root)
                && !typed
                    .cancelled_links
                    .iter()
                    .any(|link| link.starts_with(&root))
        })
}

fn warehouse_root(root: &str) -> Option<PathBuf> {
    let Reading::Local(local) = read_location(root) else {
        return None;
    };
    follow(&local.written).map(|followed| followed.resolved)
}

#[must_use]
pub fn resolve_physical_destination(path: &Path) -> Option<PathBuf> {
    let absolute = std::path::absolute(path).ok()?;
    follow(&absolute).map(|followed| followed.resolved)
}

struct Followed {
    resolved: PathBuf,
    cancelled_links: Vec<PathBuf>,
}

fn follow(absolute: &Path) -> Option<Followed> {
    let mut resolved = PathBuf::from("/");
    let mut trail: Vec<Option<PathBuf>> = Vec::new();
    let mut cancelled_links = Vec::new();
    let mut missing = false;
    for component in absolute.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Prefix(_) => return None,
            Component::ParentDir => {
                if missing {
                    return None;
                }
                if let Some(Some(link)) = trail.pop() {
                    cancelled_links.push(link);
                }
                resolved.pop();
            }
            Component::Normal(name) => {
                resolved.push(name);
                if missing {
                    continue;
                }
                match resolved.symlink_metadata() {
                    Ok(meta) if meta.file_type().is_symlink() => {
                        trail.push(Some(resolved.clone()));
                        resolved = resolved.canonicalize().ok()?;
                    }
                    Ok(_) => trail.push(None),
                    Err(error) if error.kind() == ErrorKind::NotFound => missing = true,
                    Err(_) => return None,
                }
            }
        }
    }
    Some(Followed {
        resolved,
        cancelled_links,
    })
}

#[cfg(test)]
mod tests;
