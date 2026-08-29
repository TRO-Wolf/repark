//! Namespace location resolution and scheme-selected `FileIO`.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::io::{FileIO, FileIOBuilder, LocalFsStorageFactory, StorageFactory};
use iceberg_storage_opendal::OpenDalStorageFactory;

/// Namespace warehouse key documented by RePark and Java's Glue catalog.
pub const NAMESPACE_LOCATION_PROPERTY: &str = "location";

/// Namespace key currently mapped to Glue `locationUri` by the fork. Reads use it as a fallback and
/// writes mirror the Java key onto it.
pub const NAMESPACE_LOCATION_URI_PROPERTY: &str = "location_uri";

/// ===========================================================================================
/// Resolve a namespace location with deterministic `location`, then `location_uri`, precedence.
/// ===========================================================================================
pub fn resolve_namespace_location<S: BuildHasher>(
    properties: &HashMap<String, String, S>,
) -> Option<&str> {
    properties
        .get(NAMESPACE_LOCATION_PROPERTY)
        .or_else(|| properties.get(NAMESPACE_LOCATION_URI_PROPERTY))
        .map(String::as_str)
}

/// ===========================================================================================
/// Mirror `location` to `location_uri` without clobbering explicit values.
/// ===========================================================================================
pub fn mirror_namespace_location_keys<S: BuildHasher>(properties: &mut HashMap<String, String, S>) {
    let Some(location) = properties.get(NAMESPACE_LOCATION_PROPERTY).cloned() else {
        return;
    };
    properties
        .entry(NAMESPACE_LOCATION_URI_PROPERTY.to_string())
        .or_insert(location);
}

// Scheme-based FileIO selection. URI scheme selects the storage backend; unsupported and malformed
// forms fail loud so a warehouse is never written to an unintended local path.

/// The storage backend a table-location URI scheme selects. A closed enum so the scheme→backend
/// decision is one exhaustively-tested mapping (offline; no S3 contact) rather than scattered string
/// checks — an unsupported scheme is a represented error, never a silent default that mis-places
/// data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocationBackend {
    /// `file://` or a bare **absolute** filesystem path → native local-filesystem storage.
    LocalFs,
    /// `s3://` / `s3a://` → the fork's OpenDAL S3 storage. `configured_scheme` is the exact scheme
    /// (`s3` vs `s3a`) so the storage returns object paths under the same scheme it was handed.
    ObjectStoreS3 { configured_scheme: String },
}

/// Classify a location as S3, local filesystem, or a loud unsupported-form error.
///
/// # Errors
/// Returns a plan error when `location` carries an unsupported URI scheme, a mistyped scheme (a `:`
/// before the first `/` that never formed a `scheme://`), or a non-absolute bare path.
pub(crate) fn classify_location_backend(location: &str) -> Result<LocationBackend> {
    let Some((scheme, _rest)) = location.split_once("://") else {
        // No `scheme://` — the location must be a bare *absolute* filesystem path. Reject the
        // malformed shapes (a mistyped scheme, a relative path) loud rather than silently
        // classifying them as local storage and mis-placing a warehouse's data (audit F-BR-3).
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

/// Classify a scheme-less location. It must be absolute; a pre-slash colon indicates a mistyped
/// URI. Colons after the first slash remain valid POSIX path characters.
///
/// # Errors
/// Returns a plan error for either malformed shape, naming the offending location and the supported
/// location forms.
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

/// Whether `location` has a `:` before its first `/` — or a `:` and no `/` at all. That is the shape
/// of a mistyped URI scheme (`s3:/bucket`, `s3:bucket`) that never formed a full `scheme://`; a `:`
/// only after the first `/` is a legal path-segment character and returns `false`.
pub(crate) fn has_colon_before_first_slash(location: &str) -> bool {
    match (location.find(':'), location.find('/')) {
        (Some(colon), Some(slash)) => colon < slash,
        (Some(_), None) => true,
        (None, _) => false,
    }
}

/// The [`StorageFactory`] a table `location`'s scheme selects — the one place a concrete storage
/// factory is instantiated. `s3://`/`s3a://` → the fork's [`OpenDalStorageFactory::S3`] (AWS-SDK
/// credential chain, the shape the Glue / S3 Tables catalogs use); `file://`/bare path →
/// [`LocalFsStorageFactory`]. Building the factory performs **no** network call — the S3 storage
/// resolves credentials and contacts S3 lazily on first `FileIO` use.
///
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

/// A [`FileIO`] for `location`, built from the scheme-selected [`storage_factory_for_location`] and
/// configured with `props` — the owning catalog's load-time properties (region / endpoint /
/// credentials). The S3 backend reads the `s3.*` / region keys it recognizes and resolves the rest
/// through the AWS default chain; the local backend's factory takes no config, so threading `props`
/// there is a no-op and the offline/local path stays behaviour-identical to a props-free build.
///
/// # Errors
/// Propagates [`classify_location_backend`]'s error for an unsupported scheme.
pub fn file_io_for_location<S: BuildHasher>(
    location: &str,
    props: &HashMap<String, String, S>,
) -> Result<FileIO> {
    let factory = storage_factory_for_location(location)?;
    Ok(FileIOBuilder::new(factory).with_props(props.iter()).build())
}
