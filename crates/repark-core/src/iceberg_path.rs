use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::prelude::DataFrame;
use iceberg::io::{FileIO, FileInfo};
use iceberg::table::StaticTable;
use iceberg::{NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;
use repark_common::{Error, Result};

use crate::{ReparkSession, engine_err, iceberg_err};

const HINT_WALK_LIMIT: u64 = 10_000;

const HADOOP_METADATA_SUFFIXES: [&str; 3] =
    [".metadata.json", ".gz.metadata.json", ".metadata.json.gz"];

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn read_iceberg_path(&self, path: &str) -> Result<DataFrame> {
        refuse_file_authority(path)?;
        refuse_relative_file_path(path)?;
        let location = local_file_location(path);
        let file_io = repark_iceberg::catalog::file_io_for_location(
            &location,
            &HashMap::<String, String>::new(),
        )
        .map_err(engine_err)?;
        let metadata_location = resolve_metadata_location(&file_io, &location, path).await?;
        let table = StaticTable::from_metadata_file(
            &metadata_location,
            path_table_ident(&location),
            file_io,
        )
        .await
        .map_err(iceberg_err)?
        .into_table();
        let provider: Arc<dyn TableProvider> = Arc::new(
            IcebergStaticTableProvider::try_new_from_table(table)
                .await
                .map_err(iceberg_err)?
                .with_uuid_as_string(true),
        );
        self.context().read_table(provider).map_err(engine_err)
    }
}

fn refuse_file_authority(path: &str) -> Result<()> {
    let has_authority = path
        .strip_prefix("file://")
        .and_then(|rest| rest.chars().next())
        .is_some_and(|next| next != '/');
    if !has_authority {
        return Ok(());
    }
    let named = if path.ends_with(".metadata.json") {
        path.to_string()
    } else {
        format!("{}/metadata", path.strip_suffix('/').unwrap_or(path))
    };
    Err(Error::IllegalArgument(format!(
        "Wrong FS: {named}, expected: file:///"
    )))
}

fn refuse_relative_file_path(path: &str) -> Result<()> {
    let relative = path
        .strip_prefix("file:")
        .and_then(|rest| rest.chars().next())
        .is_some_and(|next| next != '/');
    if !relative {
        return Ok(());
    }
    Err(Error::IllegalArgument(format!(
        "java.net.URISyntaxException: Relative path in absolute URI: {}",
        path.strip_suffix('/').unwrap_or(path)
    )))
}

fn local_file_location(path: &str) -> Cow<'_, str> {
    let Some(rest) = path
        .get(..5)
        .filter(|scheme| scheme.eq_ignore_ascii_case("file:"))
        .and_then(|_| path.get(5..))
    else {
        return Cow::Borrowed(path);
    };
    if rest.starts_with("///") {
        return Cow::Owned(format!("file:{rest}"));
    }
    if rest.starts_with('/') && !rest.starts_with("//") {
        return Cow::Owned(format!("file://{rest}"));
    }
    Cow::Borrowed(path)
}

async fn resolve_metadata_location(file_io: &FileIO, location: &str, path: &str) -> Result<String> {
    if location.ends_with(".metadata.json") {
        return Ok(location.to_string());
    }
    let location = location.strip_suffix('/').unwrap_or(location);
    let hint_location = format!("{location}/metadata/version-hint.text");
    if file_io.exists(&hint_location).await.map_err(iceberg_err)?
        && let Some(version) = read_version_hint(file_io, &hint_location).await?
    {
        let Some(hinted) = hadoop_metadata_file(file_io, location, version).await? else {
            return Err(hinted_metadata_missing_error(
                path,
                &format!("v{version}.metadata.json"),
            ));
        };
        return newest_from_hint(file_io, location, version, hinted, path).await;
    }
    let metadata_dir = format!("{location}/metadata");
    let files: Vec<FileInfo> = file_io
        .list(&metadata_dir)
        .await
        .map_err(iceberg_err)?
        .into_iter()
        .filter(|file| is_direct_child(&file.location, &metadata_dir))
        .collect();
    latest_metadata_location(&files)
        .map(str::to_string)
        .ok_or_else(|| no_metadata_error(path))
}

async fn newest_from_hint(
    file_io: &FileIO,
    location: &str,
    hinted_version: u64,
    hinted: String,
    path: &str,
) -> Result<String> {
    let hinted_name = hinted.rsplit('/').next().unwrap_or(&hinted).to_string();
    let mut version = hinted_version;
    let mut newest = hinted;
    for _ in 0..=HINT_WALK_LIMIT {
        let Some(next_version) = next_java_int_version(version) else {
            return Ok(newest);
        };
        let Some(next) = hadoop_metadata_file(file_io, location, next_version).await? else {
            return Ok(newest);
        };
        version = next_version;
        newest = next;
    }
    Err(hint_walk_limit_error(path, &hinted_name))
}

async fn hadoop_metadata_file(
    file_io: &FileIO,
    location: &str,
    version: u64,
) -> Result<Option<String>> {
    for suffix in HADOOP_METADATA_SUFFIXES {
        let candidate = format!("{location}/metadata/v{version}{suffix}");
        if file_io.exists(&candidate).await.map_err(iceberg_err)? {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

fn next_java_int_version(version: u64) -> Option<u64> {
    i32::try_from(version)
        .ok()
        .and_then(|version| version.checked_add(1))
        .and_then(|next| u64::try_from(next).ok())
}

async fn read_version_hint(file_io: &FileIO, hint_location: &str) -> Result<Option<u64>> {
    let raw = file_io
        .new_input(hint_location)
        .map_err(iceberg_err)?
        .read()
        .await
        .map_err(iceberg_err)?;
    let Ok(text) = std::str::from_utf8(&raw) else {
        return Ok(None);
    };
    Ok(java_int_version(text.trim()))
}

fn java_int_version(digits: &str) -> Option<u64> {
    digits
        .parse::<i32>()
        .ok()
        .and_then(|version| u64::try_from(version).ok())
}

fn hinted_metadata_missing_error(path: &str, file_name: &str) -> Error {
    Error::Analysis(format!(
        "no Iceberg table found at '{path}': version-hint.text names \
         '{file_name}', which does not exist"
    ))
}

fn hint_walk_limit_error(path: &str, hinted_name: &str) -> Error {
    Error::Analysis(format!(
        "cannot resolve the Iceberg table at '{path}': more than {HINT_WALK_LIMIT} \
         metadata files follow '{hinted_name}', the version version-hint.text names"
    ))
}

fn no_metadata_error(path: &str) -> Error {
    Error::Analysis(format!(
        "no Iceberg table found at '{path}': expected metadata files under \
         '<location>/metadata' or a '<location>/metadata/version-hint.text'"
    ))
}

fn path_table_ident(path: &str) -> TableIdent {
    let trimmed = path.strip_suffix('/').unwrap_or(path);
    let base = trimmed.rsplit('/').next().unwrap_or(trimmed);
    let name = base.strip_suffix(".metadata.json").unwrap_or(base);
    TableIdent::new(NamespaceIdent::new("path".to_string()), name.to_string())
}

fn is_direct_child(location: &str, dir: &str) -> bool {
    let dir = dir.strip_prefix("file://").unwrap_or(dir);
    location
        .strip_prefix("file://")
        .unwrap_or(location)
        .rsplit_once('/')
        .is_some_and(|(parent, _)| parent == dir)
}

fn latest_metadata_location(files: &[FileInfo]) -> Option<&str> {
    files
        .iter()
        .filter_map(|file| {
            let name = file.location.rsplit('/').next()?;
            metadata_file_order(name).map(|order| (order, file.location.as_str()))
        })
        .max_by_key(|(order, _)| *order)
        .map(|(_, location)| location)
}

#[cfg(test)]
fn metadata_file_version(name: &str) -> Option<u64> {
    metadata_file_order(name).map(|(version, _)| version)
}

fn metadata_file_order(name: &str) -> Option<(u64, usize)> {
    let preferred = HADOOP_METADATA_SUFFIXES.len();
    for (index, suffix) in HADOOP_METADATA_SUFFIXES.iter().enumerate() {
        if let Some(digits) = name
            .strip_suffix(suffix)
            .and_then(|stem| stem.strip_prefix('v'))
            && is_ascii_number(digits)
        {
            return java_int_version(digits).map(|version| (version, preferred - index));
        }
    }
    let (leading, uuid) = name.strip_suffix(".metadata.json")?.split_once('-')?;
    if !is_ascii_number(leading) || !is_uuid(uuid) {
        return None;
    }
    leading.parse().ok().map(|version| (version, preferred))
}

fn is_ascii_number(candidate: &str) -> bool {
    !candidate.is_empty() && candidate.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_uuid(candidate: &str) -> bool {
    const DASHES: [usize; 4] = [8, 13, 18, 23];
    if candidate.len() != 36 {
        return false;
    }
    candidate.bytes().enumerate().all(|(index, byte)| {
        if DASHES.contains(&index) {
            byte == b'-'
        } else {
            byte.is_ascii_hexdigit()
        }
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod hint_walk_tests;
