use std::collections::HashSet;

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

use repark_common::{Error, Result};

#[allow(clippy::missing_errors_doc)]
pub(super) async fn snapshot_destination_keys(
    store: &dyn ObjectStore,
    scope: Option<&ObjectPath>,
    origin: &str,
) -> Result<HashSet<String>> {
    let mut listed = store.list(scope);
    let mut keys = HashSet::new();
    while let Some(meta) = listed.next().await {
        let meta = meta.map_err(|error| {
            Error::DataFusion(format!("cannot list S3 destination {origin}: {error}"))
        })?;
        keys.insert(meta.location.to_string());
    }
    Ok(keys)
}

#[allow(clippy::missing_errors_doc)]
pub(super) async fn delete_keys_missing_from_snapshot(
    store: &dyn ObjectStore,
    scope: Option<&ObjectPath>,
    snapshot: &HashSet<String>,
    origin: &str,
) -> Result<()> {
    let mut listed = store.list(scope);
    let mut fresh = Vec::new();
    while let Some(meta) = listed.next().await {
        let meta = meta.map_err(|error| {
            Error::DataFusion(format!("cannot list S3 destination {origin}: {error}"))
        })?;
        if !snapshot.contains(meta.location.as_ref()) {
            fresh.push(meta.location);
        }
    }
    let mut failure = None;
    for location in &fresh {
        if let Err(error) = store.delete(location).await
            && failure.is_none()
        {
            failure = Some(format!(
                "cannot delete S3 object {location} under {origin}: {error}"
            ));
        }
    }
    match failure {
        Some(message) => Err(Error::DataFusion(message)),
        None => Ok(()),
    }
}

pub(super) fn with_cleanup_note(error: Error, cleanup: &str) -> Error {
    let suffix = format!("; failed to remove partial output after the failed write: {cleanup}");
    match error {
        Error::NotImplemented(message) => Error::NotImplemented(format!("{message}{suffix}")),
        Error::DataFusion(message) => Error::DataFusion(format!("{message}{suffix}")),
        Error::Parse(message) => Error::Parse(format!("{message}{suffix}")),
        Error::Analysis(message) => Error::Analysis(format!("{message}{suffix}")),
        Error::Arithmetic(message) => Error::Arithmetic(format!("{message}{suffix}")),
        Error::IllegalArgument(message) => Error::IllegalArgument(format!("{message}{suffix}")),
        Error::NumberFormat(message) => Error::NumberFormat(format!("{message}{suffix}")),
        Error::Config(message) => Error::Config(format!("{message}{suffix}")),
        Error::Iceberg(message) => Error::Iceberg(format!("{message}{suffix}")),
        Error::CommitStateUnknown {
            message,
            operation_id,
        } => Error::CommitStateUnknown {
            message: format!("{message}{suffix}"),
            operation_id,
        },
        other => Error::DataFusion(format!("{other}{suffix}")),
    }
}

#[cfg(test)]
mod rollback_tests {
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    use super::*;

    async fn put_key(memory: &InMemory, key: &str) {
        memory
            .put(&ObjectPath::from(key), PutPayload::from("body"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rollback_removes_only_keys_missing_from_snapshot() {
        let memory = InMemory::new();
        put_key(&memory, "prefix/kept-part").await;
        put_key(&memory, "prefix/kept-success").await;
        let scope = ObjectPath::parse("prefix").unwrap();
        let snapshot = snapshot_destination_keys(&memory, Some(&scope), "origin")
            .await
            .unwrap();
        assert_eq!(snapshot.len(), 2);
        put_key(&memory, "prefix/fresh-part").await;
        put_key(&memory, "prefix/nested/fresh-part").await;
        put_key(&memory, "elsewhere/fresh-part").await;
        delete_keys_missing_from_snapshot(&memory, Some(&scope), &snapshot, "origin")
            .await
            .unwrap();
        let after = snapshot_destination_keys(&memory, Some(&scope), "origin")
            .await
            .unwrap();
        assert_eq!(after, snapshot);
        let whole = snapshot_destination_keys(&memory, None, "origin")
            .await
            .unwrap();
        assert!(whole.contains("elsewhere/fresh-part"));
        delete_keys_missing_from_snapshot(&memory, None, &snapshot, "origin")
            .await
            .unwrap();
        let cleaned = snapshot_destination_keys(&memory, None, "origin")
            .await
            .unwrap();
        assert_eq!(cleaned, snapshot);
    }

    #[tokio::test]
    async fn rollback_over_an_empty_snapshot_and_store_is_a_noop() {
        let memory = InMemory::new();
        let scope = ObjectPath::parse("prefix").unwrap();
        let snapshot = snapshot_destination_keys(&memory, Some(&scope), "origin")
            .await
            .unwrap();
        assert!(snapshot.is_empty());
        delete_keys_missing_from_snapshot(&memory, Some(&scope), &snapshot, "origin")
            .await
            .unwrap();
    }

    #[test]
    fn cleanup_note_keeps_the_original_error_variant() {
        let noted = with_cleanup_note(Error::Analysis("boom".to_string()), "cleanup failed");
        assert!(matches!(noted, Error::Analysis(_)));
        assert_eq!(
            noted.to_string(),
            "boom; failed to remove partial output after the failed write: cleanup failed"
        );
        let noted = with_cleanup_note(
            Error::CommitStateUnknown {
                message: "boom".to_string(),
                operation_id: Some("op".to_string()),
            },
            "cleanup failed",
        );
        assert!(matches!(
            noted,
            Error::CommitStateUnknown {
                operation_id: Some(_),
                ..
            }
        ));
        assert!(noted.to_string().contains("cleanup failed"));
    }
}
