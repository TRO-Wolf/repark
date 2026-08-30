//! A13: `file://` warehouse strings on the `register_memory_catalog` product path.

use super::super::*;
use crate::LocationPolicy;

/// Skipping `memory_warehouse_fallback_root` at registration (plain `PathBuf::from(warehouse)`)
/// makes this pin red. Helper-only tests are not this claim.
#[tokio::test]
async fn register_memory_catalog_file_uri_fallback_root_is_the_filesystem_path() {
    use tempfile::TempDir;

    let warehouse = TempDir::new().unwrap();
    let abs = warehouse.path().to_str().unwrap();
    let session = ReparkSession::new().unwrap();
    for uri in [
        format!("file://{abs}"),
        format!("FILE://{abs}"),
        format!("file://localhost{abs}"),
    ] {
        let name = format!(
            "ice_{}",
            uri.chars()
                .filter(char::is_ascii_alphanumeric)
                .take(24)
                .collect::<String>()
        );
        session
            .register_memory_catalog(&name, &uri)
            .await
            .unwrap_or_else(|error| panic!("register {uri}: {error}"));
        match session.catalogs_snapshot().location_policy(&name) {
            Some(LocationPolicy::TempFallbackAllowed { root }) => {
                assert_eq!(root, warehouse.path(), "uri {uri}");
            }
            other => panic!("expected TempFallbackAllowed for {uri}, got {other:?}"),
        }
    }
}
