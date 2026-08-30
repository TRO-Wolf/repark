//! Snapshot-ref helpers over the fork's `ManageSnapshots` transaction API.

use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, Result, TableIdent};

/// Kind of snapshot ref (branch vs tag).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotRefKind {
    /// A mutable branch ref.
    Branch,
    /// An immutable tag ref.
    Tag,
}

/// Optional retention fields mapped onto fork `SnapshotRetention` setters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SnapshotRefRetention {
    /// Max age of the ref itself (ms).
    pub max_ref_age_ms: Option<i64>,
    /// Branch only: min snapshots to keep while expiring.
    pub min_snapshots_to_keep: Option<i32>,
    /// Branch only: max age of snapshots in the branch (ms).
    pub max_snapshot_age_ms: Option<i64>,
}

impl SnapshotRefRetention {
    /// True when no retention field is set.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.max_ref_age_ms.is_none()
            && self.min_snapshots_to_keep.is_none()
            && self.max_snapshot_age_ms.is_none()
    }
}

/// Create a branch or tag ref pointing at `snapshot_id` on `ident`.
/// # Errors
/// Propagates any [`iceberg::Error`] from load / apply / commit.
pub async fn create_snapshot_ref(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: SnapshotRefKind,
    name: &str,
    snapshot_id: i64,
) -> Result<()> {
    create_snapshot_ref_with_retention(
        catalog,
        ident,
        kind,
        name,
        snapshot_id,
        SnapshotRefRetention::default(),
    )
    .await
}

/// Create a branch or tag with optional retention (same transaction as create).
/// # Errors
/// Propagates any [`iceberg::Error`] from load / apply / commit.
pub async fn create_snapshot_ref_with_retention(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: SnapshotRefKind,
    name: &str,
    snapshot_id: i64,
    retention: SnapshotRefRetention,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let action = match kind {
        SnapshotRefKind::Branch => tx.manage_snapshots().create_branch(name, snapshot_id),
        SnapshotRefKind::Tag => tx.manage_snapshots().create_tag(name, snapshot_id),
    };
    let action = apply_retention(action, name, retention);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// Replace (re-pin) an existing branch or tag at `snapshot_id`.
/// # Errors
/// Propagates any [`iceberg::Error`] from load / apply / commit.
pub async fn replace_snapshot_ref(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: SnapshotRefKind,
    name: &str,
    snapshot_id: i64,
    retention: SnapshotRefRetention,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let action = match kind {
        SnapshotRefKind::Branch => tx.manage_snapshots().replace_branch(name, snapshot_id),
        SnapshotRefKind::Tag => tx.manage_snapshots().replace_tag(name, snapshot_id),
    };
    let action = apply_retention(action, name, retention);
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// CREATE OR REPLACE: replace when the named ref already exists as the requested kind; else create.
/// # Errors
/// Propagates any [`iceberg::Error`] from load / apply / commit.
pub async fn create_or_replace_snapshot_ref(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: SnapshotRefKind,
    name: &str,
    snapshot_id: i64,
    retention: SnapshotRefRetention,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let exists = table.metadata().snapshot_for_ref(name).is_some();
    if exists {
        replace_snapshot_ref(catalog, ident, kind, name, snapshot_id, retention).await
    } else {
        create_snapshot_ref_with_retention(catalog, ident, kind, name, snapshot_id, retention).await
    }
}

/// Drop a branch or tag ref on `ident`.
/// # Errors
/// Propagates any [`iceberg::Error`] from load / apply / commit.
pub async fn drop_snapshot_ref(
    catalog: &dyn Catalog,
    ident: &TableIdent,
    kind: SnapshotRefKind,
    name: &str,
) -> Result<()> {
    let table = catalog.load_table(ident).await?;
    let tx = Transaction::new(&table);
    let action = match kind {
        SnapshotRefKind::Branch => tx.manage_snapshots().remove_branch(name),
        SnapshotRefKind::Tag => tx.manage_snapshots().remove_tag(name),
    };
    let tx = action.apply(tx)?;
    tx.commit(catalog).await?;
    Ok(())
}

/// Chain fork retention setters onto a manage-snapshots action.
fn apply_retention(
    mut action: iceberg::transaction::ManageSnapshotsAction,
    name: &str,
    retention: SnapshotRefRetention,
) -> iceberg::transaction::ManageSnapshotsAction {
    if let Some(max_ref_age_ms) = retention.max_ref_age_ms {
        action = action.set_max_ref_age_ms(name, max_ref_age_ms);
    }
    if let Some(min_snapshots) = retention.min_snapshots_to_keep {
        action = action.set_min_snapshots_to_keep(name, min_snapshots);
    }
    if let Some(max_snapshot_age_ms) = retention.max_snapshot_age_ms {
        action = action.set_max_snapshot_age_ms(name, max_snapshot_age_ms);
    }
    action
}
