use std::collections::{BTreeSet, HashMap};

use tempfile::TempDir;

use crate::catalog::{CatalogCaches, IcebergCacheSettings, memory_catalog_cached_with_props};
use crate::tests::tracing::{SpanEvent, begin_catalog_capture, clear_catalog_capture_slot};

struct ClearOnDrop;

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        clear_catalog_capture_slot();
    }
}

const SPAN: &str = "catalog.memory_catalog_cached_with_props";

#[tokio::test]
async fn props_builder_span_matches_cached_fields_and_records_no_props() {
    let capture = begin_catalog_capture();
    let _clear = ClearOnDrop;
    let warehouse = TempDir::new().unwrap();
    let props = HashMap::from([
        ("metadata-naming".to_string(), "hadoop".to_string()),
        (
            "s3.secret-access-key".to_string(),
            "SUPER_SECRET_hpin".to_string(),
        ),
    ]);
    memory_catalog_cached_with_props(
        warehouse.path().to_str().unwrap(),
        &CatalogCaches::new(IcebergCacheSettings::default()),
        &props,
    )
    .await
    .unwrap();
    let events: Vec<SpanEvent> = capture
        .snapshot()
        .into_iter()
        .filter(|(name, _)| name == SPAN)
        .collect();
    assert_eq!(events.len(), 1, "{events:?}");
    let fields = &events[0].1;
    let names: BTreeSet<&str> = fields.iter().map(|(name, _)| name.as_str()).collect();
    assert_eq!(
        names,
        BTreeSet::from([
            "warehouse",
            "metadata_cache",
            "manifest_cache_bytes",
            "footer_cache"
        ]),
        "{fields:?}"
    );
    for (_, value) in fields {
        for forbidden in ["hadoop", "metadata-naming", "SUPER_SECRET_hpin", "secret"] {
            assert!(!value.contains(forbidden), "{forbidden} in {fields:?}");
        }
    }
}
