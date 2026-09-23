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

impl ReparkSession {
    #[allow(clippy::missing_errors_doc)]
    pub async fn read_iceberg_path(&self, path: &str) -> Result<DataFrame> {
        let file_io =
            repark_iceberg::catalog::file_io_for_location(path, &HashMap::<String, String>::new())
                .map_err(engine_err)?;
        let metadata_location = resolve_metadata_location(&file_io, path).await?;
        let table =
            StaticTable::from_metadata_file(&metadata_location, path_table_ident(path), file_io)
                .await
                .map_err(iceberg_err)?
                .into_table();
        let provider: Arc<dyn TableProvider> = Arc::new(
            IcebergStaticTableProvider::try_new_from_table(table)
                .await
                .map_err(iceberg_err)?,
        );
        self.context().read_table(provider).map_err(engine_err)
    }
}

async fn resolve_metadata_location(file_io: &FileIO, path: &str) -> Result<String> {
    if path.ends_with(".metadata.json") {
        return Ok(path.to_string());
    }
    let location = path.strip_suffix('/').unwrap_or(path);
    let hint_location = format!("{location}/metadata/version-hint.text");
    if file_io.exists(&hint_location).await.map_err(iceberg_err)?
        && let Some(version) = read_version_hint(file_io, &hint_location).await?
    {
        let file_name = format!("v{version}.metadata.json");
        let hinted = format!("{location}/metadata/{file_name}");
        if file_io.exists(&hinted).await.map_err(iceberg_err)? {
            return Ok(hinted);
        }
        return Err(hinted_metadata_missing_error(path, &file_name));
    }
    let files = file_io
        .list(format!("{location}/metadata"))
        .await
        .map_err(iceberg_err)?;
    latest_metadata_location(&files)
        .map(str::to_string)
        .ok_or_else(|| no_metadata_error(path))
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
    Ok(text.trim().parse().ok())
}

fn hinted_metadata_missing_error(path: &str, file_name: &str) -> Error {
    Error::Analysis(format!(
        "no Iceberg table found at '{path}': version-hint.text names \
         '{file_name}', which does not exist"
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

fn latest_metadata_location(files: &[FileInfo]) -> Option<&str> {
    files
        .iter()
        .filter_map(|file| {
            let name = file.location.rsplit('/').next()?;
            metadata_file_version(name).map(|version| (version, file.location.as_str()))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, location)| location)
}

fn metadata_file_version(name: &str) -> Option<u64> {
    let stem = name.strip_suffix(".metadata.json")?;
    if let Some(rest) = stem.strip_prefix('v') {
        if !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()) {
            return rest.parse().ok();
        }
        return None;
    }
    let (leading, uuid) = stem.split_once('-')?;
    if leading.is_empty() || !leading.bytes().all(|b| b.is_ascii_digit()) || !is_uuid(uuid) {
        return None;
    }
    leading.parse().ok()
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
