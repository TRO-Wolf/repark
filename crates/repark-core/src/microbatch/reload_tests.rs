use std::sync::Arc;

use crate::microbatch::driver::{BatchBody, SinkSpec, StreamSpec, StreamingQueryManager, Trigger};
use crate::microbatch::lifecycle_tests::{Mode, ONE, Probe, ended};
use crate::microbatch::table_door_tests::{FLAKY_SINK, flaky};
use crate::microbatch::testing::{Fixture, SOURCE, options, stamped_epochs};

const FORK_REFRESH: usize = 1;

#[tokio::test]
async fn the_driver_loads_the_sink_fresh_for_every_batch() {
    for foreach in [false, true] {
        let fixture = Fixture::new().await;
        for value in 1..=3 {
            fixture.insert(SOURCE, &format!("({value})")).await;
        }
        let catalog = flaky(&fixture).await;
        let body = Probe::new(Mode::Record);
        let sink = FLAKY_SINK.to_string();
        let sink = if foreach {
            SinkSpec::ForeachBatch {
                sink,
                body: Arc::clone(&body) as Arc<dyn BatchBody>,
            }
        } else {
            SinkSpec::Table { sink }
        };
        let mut spec = StreamSpec::new(SOURCE, options(ONE), sink);
        spec.trigger = Trigger::AvailableNow;
        let handle = StreamingQueryManager::of(&fixture.session)
            .register(&fixture.session, spec)
            .await
            .expect("register");
        let registered = catalog.events().len();
        handle.start_below_catalog_check().expect("start");
        assert_eq!(ended(&handle).await, Ok(true));
        assert_eq!(stamped_epochs(&fixture.table("silver").await), [0, 1, 2]);
        let events = catalog.events().split_off(registered);
        let loads_before_each_commit: Vec<usize> = events
            .split(|event| event == "commit silver")
            .map(<[String]>::len)
            .collect();
        let driver_loads = if foreach { 2 } else { 1 };
        assert_eq!(
            loads_before_each_commit,
            [
                1 + driver_loads + FORK_REFRESH,
                driver_loads + FORK_REFRESH,
                driver_loads + FORK_REFRESH,
                0
            ],
            "foreachBatch: {foreach}: {events:?}"
        );
        assert!(events.iter().all(|event| event.ends_with(" silver")));
    }
}
