//! Namespace location property resolution and scheme-selected `FileIO`.
//!
//! Extracted MOVE-ONLY from `lib.rs` (r25 T0). Zero behavior change.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::io::{FileIO, FileIOBuilder, LocalFsStorageFactory, StorageFactory};
use iceberg_storage_opendal::OpenDalStorageFactory;

/// The namespace property key `RePark` documents for a namespace's warehouse path — and the key
/// Java's `GlueCatalog` maps to the Glue database `locationUri` (apache-iceberg-1.10.0
/// `IcebergToGlueConverter.GLUE_DB_LOCATION_KEY = "location"`, `toDatabaseInput`;
/// `GlueCatalog.loadNamespaceMetadata` maps `locationUri` back to it).
pub const NAMESPACE_LOCATION_PROPERTY: &str = "location";

/// The namespace property key the **fork's** Glue catalog currently maps to the Glue database
/// `locationUri` (fork `catalog/glue/src/utils.rs:42` at pin `fe30d7d4` — a divergence from the
/// Java key above; the Java-parity ask is filed in `task/todo.md`, U2 follow-ups). `RePark` reads
/// it as a fallback and mirrors it on write so the canonical Glue field is set under BOTH mappings.
pub const NAMESPACE_LOCATION_URI_PROPERTY: &str = "location_uri";

/// ===========================================================================================
/// Resolve a namespace's warehouse location from its property map — the ONE read path for the
/// Glue namespace-location key identity (audit BUG-001 / U2).
///
/// Precedence is deterministic and documented: [`NAMESPACE_LOCATION_PROPERTY`] (`"location"`,
/// the Java-canonical key `RePark` documents) first, [`NAMESPACE_LOCATION_URI_PROPERTY`]
/// (`"location_uri"`, the key the fork's Glue catalog fills from a real Glue database's
/// `locationUri`) as the fallback — never an iteration-order pick. So a pre-existing Glue
/// database (only `location_uri`) resolves, a legacy `RePark`-created namespace (only
/// `location`) resolves, and when both are set (the post-U2 dual-write shape) `location` wins.
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
/// Mirror a to-be-created namespace's `location` property onto `location_uri` — the ONE write
/// helper for the Glue namespace-location key identity (audit BUG-001 / U2), called by every
/// `RePark` namespace-create path (programmatic `create_namespace` and SQL `CREATE NAMESPACE`).
///
/// Unidirectional and non-clobbering: copies [`NAMESPACE_LOCATION_PROPERTY`] to
/// [`NAMESPACE_LOCATION_URI_PROPERTY`] only when the former is present and the latter absent. It
/// never synthesizes `location` from an explicit `location_uri` (a caller hand-setting the fork's
/// key gets exactly the map they wrote), never overwrites an explicitly-set key, and leaves a
/// location-less map untouched. Under the fork's current mapping `location_uri` becomes the Glue
/// database `locationUri` (what other engines and the fork's own default-table-path read) while
/// `location` rides along as a plain parameter; under Java's mapping the roles swap — either way
/// the canonical Glue field is set.
/// ===========================================================================================
pub fn mirror_namespace_location_keys<S: BuildHasher>(properties: &mut HashMap<String, String, S>) {
    let Some(location) = properties.get(NAMESPACE_LOCATION_PROPERTY).cloned() else {
        return;
    };
    properties
        .entry(NAMESPACE_LOCATION_URI_PROPERTY.to_string())
        .or_insert(location);
}

// ===========================================================================================
// Scheme-based FileIO selection.
//
// A table's storage location is a URI whose scheme decides which storage backend must serve it:
// an `s3://` / `s3a://` warehouse needs the fork's OpenDAL S3 storage (the AWS-SDK credential
// chain — exactly the shape the Glue / S3 Tables catalogs build their own FileIO with), while a
// `file://` URI or a bare filesystem path needs the native local-filesystem storage. The `Catalog`
// trait does not expose the FileIO it was built with, so any write path that must construct its
// own FileIO (the staged-CTAS create arm; the in-memory catalog builder) derives the backend from
// the location here — the single place a concrete storage factory is chosen, so no call site
// hardcodes one.
// ===========================================================================================

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

/// Classify a table `location`'s URI scheme into the [`LocationBackend`] that must serve it.
///
/// `s3://`/`s3a://` → S3; `file://` or a bare **absolute** path → local filesystem; any other
/// scheme is a loud, actionable error naming the scheme and the supported set (never a silent
/// fallback that would write a real warehouse's data to the wrong place). A location with no
/// `scheme://` is validated by [`classify_bare_location`] — a single-slash scheme typo
/// (`s3:/bucket`) or a relative path is rejected loud, not silently treated as local storage
/// (audit F-BR-3).
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

/// Classify a `location` that carried no `scheme://` — it must be a bare **absolute** filesystem
/// path. Two malformed shapes are rejected loud rather than silently mis-classified as local
/// storage (audit F-BR-3, S2 — the bare-path arm previously accepted anything without a `://`, so a
/// single-slash scheme typo or a relative path became [`LocationBackend::LocalFs`] and a strict
/// catalog's CTAS published a broken table under a CWD-relative directory):
/// - a `:` before the first `/` — a mistyped URI scheme that never formed a real `scheme://`
///   (`s3:/bucket/wh`, `s3a:/x`, `s3:bucket`), pointed back at `scheme://`;
/// - a path that is not `/`-prefixed — a relative location, or the empty string, that would resolve
///   against the process working directory.
///
/// A `:` that appears only *after* the first `/` is a legal POSIX path character and is allowed
/// (`/data/ns:v2/t` → local filesystem).
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
