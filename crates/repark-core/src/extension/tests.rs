//! Hook ORDER pins for [`SessionExtension`] during `ReparkSessionBuilder::build`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::prelude::{SessionConfig, SessionContext};

use super::SessionExtension;
use crate::ReparkSession;

/// Records hook invocation order and leaves an observable mark in each hook position:
/// `configure` amends the `SessionConfig` (batch size 1234) so its output reaching the live
/// context proves the hook ran BEFORE context assembly; `register` asserts it sees that amended
/// config on the context, proving it ran AFTER context creation.
struct RecordingExtension {
    events: Arc<Mutex<Vec<&'static str>>>,
}

impl SessionExtension for RecordingExtension {
    fn configure(
        &self,
        conf: &HashMap<String, String>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("configure");
        assert_eq!(
            conf.get("test.marker").map(String::as_str),
            Some("on"),
            "configure must receive the builder's FULL config map (v1 parity: the inline \
             cardinality install parsed the same map)"
        );
        Ok(config.with_batch_size(1234))
    }

    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push("register");
        assert_eq!(
            ctx.copied_config().options().execution.batch_size,
            1234,
            "register must run on a context assembled FROM the configure-amended SessionConfig \
             (configure before RuntimeEnv/context assembly, register after context creation)"
        );
        Ok(())
    }
}

/// PIN — hook ORDER during `build()`: `configure` first (pre-assembly), `register` second
/// (post-context), each exactly once, and the configure-amended config is live on the built
/// session. Risk covered: a re-home reordering the hooks or dropping the configure output on
/// the floor.
#[tokio::test]
async fn build_runs_configure_then_register_at_the_v1_inline_positions() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let session = ReparkSession::builder()
        .config("test.marker", "on")
        .with_extension(Arc::new(RecordingExtension {
            events: events.clone(),
        }))
        .build()
        .expect("build with a recording extension");
    assert_eq!(
        *events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        vec!["configure", "register"],
        "exactly one configure then exactly one register"
    );
    assert_eq!(
        session
            .context()
            .copied_config()
            .options()
            .execution
            .batch_size,
        1234,
        "the configure-amended SessionConfig must be the one the live context was built from"
    );
}

/// PIN — the no-extension build stays the pure-DataFusion baseline: both hooks default to
/// no-ops and a trivial `sql()` works.
#[tokio::test]
async fn default_hooks_are_noops_and_build_succeeds_without_extension() {
    let session = ReparkSession::new().expect("no-extension build");
    let frame = session
        .sql("SELECT 1 AS one")
        .await
        .expect("plain DataFusion sql on the default session");
    let batches = frame.collect().await.expect("collect");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}
