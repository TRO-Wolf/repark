use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjectPath;

use repark_common::{Error, Result};

#[allow(clippy::missing_errors_doc)]
pub(super) async fn delete_recorded_keys(
    store: &dyn ObjectStore,
    locations: &[ObjectPath],
    origin: &str,
) -> Result<()> {
    let mut failure = None;
    for location in locations {
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

pub(super) fn cleanup_message(cleanup: &Error) -> String {
    match cleanup {
        Error::DataFusion(message) => message.clone(),
        other => other.to_string(),
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
    use std::collections::HashSet;

    use futures::StreamExt;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    use super::*;

    async fn put_key(memory: &InMemory, key: &str) {
        memory
            .put(&ObjectPath::from(key), PutPayload::from("body"))
            .await
            .unwrap();
    }

    async fn listed(memory: &InMemory) -> HashSet<String> {
        let mut listed = memory.list(None);
        let mut keys = HashSet::new();
        while let Some(meta) = listed.next().await {
            keys.insert(meta.unwrap().location.to_string());
        }
        keys
    }

    #[tokio::test]
    async fn rollback_deletes_only_the_recorded_keys() {
        let memory = InMemory::new();
        put_key(&memory, "prefix/kept-part").await;
        put_key(&memory, "prefix/zz-foreign-same").await;
        put_key(&memory, "elsewhere/zz-foreign-other").await;
        put_key(&memory, "prefix/fresh-part").await;
        put_key(&memory, "prefix/nested/fresh-part").await;
        let recorded = vec![
            ObjectPath::from("prefix/fresh-part"),
            ObjectPath::from("prefix/nested/fresh-part"),
        ];
        delete_recorded_keys(&memory, &recorded, "origin")
            .await
            .unwrap();
        let after = listed(&memory).await;
        assert!(!after.contains("prefix/fresh-part"));
        assert!(!after.contains("prefix/nested/fresh-part"));
        assert!(after.contains("prefix/kept-part"));
        assert!(after.contains("prefix/zz-foreign-same"));
        assert!(after.contains("elsewhere/zz-foreign-other"));
    }

    #[tokio::test]
    async fn rollback_with_no_recorded_keys_is_a_noop() {
        let memory = InMemory::new();
        put_key(&memory, "prefix/kept-part").await;
        delete_recorded_keys(&memory, &[], "origin").await.unwrap();
        let after = listed(&memory).await;
        assert_eq!(after.len(), 1);
        assert!(after.contains("prefix/kept-part"));
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

    #[test]
    fn cleanup_note_carries_the_bare_cleanup_message_once() {
        let cleanup = Error::DataFusion("cannot delete S3 object k under o: denied".to_string());
        assert_eq!(
            cleanup_message(&cleanup),
            "cannot delete S3 object k under o: denied"
        );
        let noted = with_cleanup_note(
            Error::DataFusion("boom".to_string()),
            &cleanup_message(&cleanup),
        );
        assert_eq!(
            noted
                .to_string()
                .matches("datafusion engine error:")
                .count(),
            1
        );
        assert!(noted.to_string().contains("cannot delete S3 object k"));
    }
}
