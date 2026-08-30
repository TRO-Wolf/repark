//! Namespace location resolution and scheme-selected `FileIO`.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::io::{FileIO, FileIOBuilder, LocalFsStorageFactory, StorageFactory};
use iceberg_storage_opendal::OpenDalStorageFactory;

/// Namespace warehouse key documented by `RePark` and Java's Glue catalog.
pub const NAMESPACE_LOCATION_PROPERTY: &str = "location";

/// Namespace key currently mapped to Glue `locationUri` by the fork.
pub const NAMESPACE_LOCATION_URI_PROPERTY: &str = "location_uri";

/// Resolve a namespace location with deterministic `location`, then `location_uri`, precedence.
pub fn resolve_namespace_location<S: BuildHasher>(
    properties: &HashMap<String, String, S>,
) -> Option<&str> {
    properties
        .get(NAMESPACE_LOCATION_PROPERTY)
        .or_else(|| properties.get(NAMESPACE_LOCATION_URI_PROPERTY))
        .map(String::as_str)
}

/// Mirror `location` to `location_uri` without clobbering explicit values.
pub fn mirror_namespace_location_keys<S: BuildHasher>(properties: &mut HashMap<String, String, S>) {
    let Some(location) = properties.get(NAMESPACE_LOCATION_PROPERTY).cloned() else {
        return;
    };
    properties
        .entry(NAMESPACE_LOCATION_URI_PROPERTY.to_string())
        .or_insert(location);
}

// Scheme-based FileIO selection.

/// The storage backend a table-location URI scheme selects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocationBackend {
    /// `file://` or a bare **absolute** filesystem path → native local-filesystem storage.
    LocalFs,
    /// `s3://` / `s3a://` → the fork's OpenDAL S3 storage.
    ObjectStoreS3 { configured_scheme: String },
}

/// Classify a location as S3, local filesystem, or a loud unsupported-form error.
/// # Errors
/// Returns a plan error when `location` carries an unsupported URI scheme, a mistyped scheme (a
pub(crate) fn classify_location_backend(location: &str) -> Result<LocationBackend> {
    let Some((scheme, _rest)) = location.split_once("://") else {
        // No `scheme://` — the location must be a bare *absolute* filesystem path.
        return classify_bare_location(location);
    };
    let scheme = scheme.to_ascii_lowercase();
    match scheme.as_str() {
        "file" => Ok(LocationBackend::LocalFs),
        "s3" | "s3a" => Ok(LocationBackend::ObjectStoreS3 {
            configured_scheme: scheme,
        }),
        other => Err(DataFusionError::Plan(format!(
            "unsupported storage scheme `{other}://` in location `{location}`: RePark selects a \
             `FileIO` backend by scheme and supports `s3://`, `s3a://`, `file://`, or a bare \
             absolute filesystem path"
        ))),
    }
}

/// Classify a scheme-less location.
/// # Errors
/// Returns a plan error naming the malformed location and the supported location forms.
pub(crate) fn classify_bare_location(location: &str) -> Result<LocationBackend> {
    if has_colon_before_first_slash(location) {
        return Err(DataFusionError::Plan(format!(
            "malformed storage location `{location}`: a `:` before the first `/` looks like a \
             mistyped URI scheme — did you mean `scheme://…`? RePark supports `s3://`, `s3a://`, \
             `file://`, or a bare absolute filesystem path"
        )));
    }
    if !location.starts_with('/') {
        return Err(DataFusionError::Plan(format!(
            "storage location `{location}` is not an absolute path: a bare filesystem warehouse \
             path must start with `/`. RePark supports `s3://`, `s3a://`, `file://`, or a bare \
             absolute filesystem path"
        )));
    }
    Ok(LocationBackend::LocalFs)
}

/// Whether `location` has a `:` before its first `/` — or a `:` and no `/` at all.
pub(crate) fn has_colon_before_first_slash(location: &str) -> bool {
    match (location.find(':'), location.find('/')) {
        (Some(colon), Some(slash)) => colon < slash,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// The [`StorageFactory`] a table `location`'s scheme selects.
/// # Errors
/// Propagates [`classify_location_backend`]'s error for an unsupported scheme.
pub fn storage_factory_for_location(location: &str) -> Result<Arc<dyn StorageFactory>> {
    match classify_location_backend(location)? {
        LocationBackend::LocalFs => Ok(Arc::new(LocalFsStorageFactory)),
        LocationBackend::ObjectStoreS3 { configured_scheme } => {
            Ok(Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme,
                customized_credential_load: None,
            }))
        }
    }
}

/// A [`FileIO`] for `location`, built from the scheme-selected factory and catalog `props`.
/// # Errors
/// Propagates [`classify_location_backend`]'s error for an unsupported scheme.
pub fn file_io_for_location<S: BuildHasher>(
    location: &str,
    props: &HashMap<String, String, S>,
) -> Result<FileIO> {
    let factory = storage_factory_for_location(location)?;
    Ok(FileIOBuilder::new(factory).with_props(props.iter()).build())
}
