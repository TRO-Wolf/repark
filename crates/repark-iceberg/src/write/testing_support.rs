//! Test-support-only Iceberg snapshot-ref helpers.
//!
//! **Still a test-support seam** for fixtures that prefer the `_testing_create_ref` API. Public
//! SQL `CREATE BRANCH` / `CREATE TAG` is product surface as of I5 (see [`crate::write::snapshot_refs`]);
//! this module remains so existing tests keep working without expanding the Python public API.
//!
//! Fork: `Transaction::manage_snapshots().create_branch|create_tag` + `apply` + `commit`
//! (`crates/iceberg/src/transaction/manage_snapshots.rs:90-108`, pin `b009ac15` / R97–R98).

use iceberg::{Catalog, Result, TableIdent};

pub use crate::write::snapshot_refs::SnapshotRefKind;
use crate::write::snapshot_refs::create_snapshot_ref;

/// ===========================================================================================
/// Create a branch or tag ref pointing at `snapshot_id` on `ident` (test-support only).
///
/// Thin seam over fork `ManageSnapshotsAction::create_branch` / `create_tag`. Fails if the ref
/// already exists or the snapshot id is unknown (fork validation).
/// ===========================================================================================
///
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
