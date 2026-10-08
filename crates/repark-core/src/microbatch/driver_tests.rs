use std::sync::Arc;

use repark_iceberg::microbatch::offset::{QueryId, TableUuid};

use super::*;
use crate::microbatch::testing::{Fixture, SINK, SOURCE, options};

fn table_spec(sink: &str) -> StreamSpec {
    StreamSpec::new(
        SOURCE,
        options(&[]),
        SinkSpec::Table {
            sink: sink.to_string(),
        },
    )
}

#[tokio::test]
async fn the_manager_rides_the_session_config_once_per_session() {
    let fixture = Fixture::new().await;
    let first = StreamingQueryManager::of(&fixture.session);
    let second = StreamingQueryManager::of(&fixture.session);
    assert!(Arc::ptr_eq(&first, &second));
    let installed = fixture
        .session
        .context()
        .state_ref()
        .read()
        .config()
        .get_extension::<StreamingQueryManager>()
        .expect("the manager is a config extension");
    assert!(Arc::ptr_eq(&first, &installed));
    let other = Fixture::new().await;
    assert!(!Arc::ptr_eq(
        &first,
        &StreamingQueryManager::of(&other.session)
    ));
}

#[tokio::test]
async fn registering_derives_the_query_id_from_the_sink_and_does_not_start() {
    let fixture = Fixture::new().await;
    let manager = StreamingQueryManager::of(&fixture.session);
    let sink = TableUuid::of(&fixture.table("silver").await);
    let mut spec = table_spec(SINK);
    spec.query_name = Some("orders_to_silver".to_string());
    let handle = manager
        .register(&fixture.session, spec)
        .await
        .expect("register");
    assert_eq!(handle.id(), QueryId::derive(sink, Some("orders_to_silver")));
    assert_eq!(handle.name().as_deref(), Some("orders_to_silver"));
    assert_eq!(handle.sink(), SINK);
    assert_eq!(handle.state(), QueryState::Registered);
    assert!(!handle.is_active());
    assert!(manager.active().is_empty());
    let unnamed = manager
        .register(&fixture.session, table_spec(SINK))
        .await
        .expect("register unnamed");
    assert_eq!(unnamed.id(), QueryId::derive(sink, None));
    assert_ne!(unnamed.run_id(), handle.run_id());
}

#[tokio::test]
async fn registering_refuses_an_unknown_or_malformed_sink() {
    let fixture = Fixture::new().await;
    let manager = StreamingQueryManager::of(&fixture.session);
    for (sink, needle) in [
        ("ice.sales.missing", "cannot load table"),
        ("nope.sales.silver", "is not registered"),
        ("ice.silver", "must be catalog.namespace.table"),
    ] {
        let error = manager
            .register(&fixture.session, table_spec(sink))
            .await
            .expect_err("an unresolvable sink refuses");
        assert!(
            matches!(&error, MicroBatchError::Catalog(message) if message.contains(needle)),
            "{sink}: {error:?}"
        );
    }
}

#[test]
fn a_recorded_location_never_renders() {
    let location = RecordedLocation::new("s3://bucket/checkpoints/q?token=secret");
    let rendered = format!("{location:?}");
    assert_eq!(rendered, "RecordedLocation(<redacted>)");
    let spec = StreamSpec {
        checkpoint_location: Some(location),
        ..table_spec(SINK)
    };
    let rendered = format!("{spec:?}");
    assert!(!rendered.contains("bucket"), "{rendered}");
    assert!(!rendered.contains("secret"), "{rendered}");
}

#[test]
fn query_states_split_active_from_terminal() {
    let active: Vec<QueryState> = [
        QueryState::Registered,
        QueryState::Running,
        QueryState::Draining,
        QueryState::Stopped,
        QueryState::Failed,
        QueryState::RecoveryRequired,
    ]
    .into_iter()
    .filter(|state| state.is_active())
    .collect();
    assert_eq!(active, [QueryState::Running, QueryState::Draining]);
    assert!(!QueryState::Registered.is_terminal());
    assert!(QueryState::RecoveryRequired.is_terminal());
    assert_eq!(Trigger::default(), Trigger::ProcessingTime(Duration::ZERO));
}
