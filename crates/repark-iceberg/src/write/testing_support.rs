//! Test-support-only Iceberg snapshot-ref helpers.

use iceberg::{Catalog, Result, TableIdent};

pub use crate::write::snapshot_refs::SnapshotRefKind;
use crate::write::snapshot_refs::create_snapshot_ref;

/// Create a branch or tag ref pointing at `snapshot_id` on `ident` (test-support only).
/// # Errors
/// Propagates any [`iceberg::Error`] from load / apply / commit.
pub async fn testing_create_ref(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: SnapshotRefKind,
    name: &str,
    snapshot_id: i64,
) -> Result<()> {
    create_snapshot_ref(catalog, ident, kind, name, snapshot_id).await
}
