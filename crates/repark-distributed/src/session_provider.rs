use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_scheduler::SessionBuilder;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::{Error, ReparkSession, Result, parallel_single_partition_active};

#[derive(Clone)]
pub struct ReparkSessionProvider {
    state: SessionState,
}

impl ReparkSessionProvider {
    #[allow(clippy::missing_errors_doc)]
    pub fn from_session(session: &ReparkSession) -> Result<Self> {
        Self::from_context(session.context())
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn from_context(context: &SessionContext) -> Result<Self> {
        let state = context.state();
        if parallel_single_partition_active(&state) {
            return Err(Error::Config(
                "ReparkSessionProvider refuses a session with single-partition parallelism on: \
                 ParallelWindowExec has no distributed codec arm, and the plans a cluster runs \
                 are built on this session's own context; build the session with \
                 ReparkSessionBuilder::parallel_single_partition(false)"
                    .to_owned(),
            ));
        }
        Ok(Self { state })
    }

    #[must_use]
    pub fn session_state(&self) -> SessionState {
        self.state.clone()
    }

    #[must_use]
    pub fn session_builder(&self) -> SessionBuilder {
        let state = self.state.clone();
        Arc::new(move |_: SessionConfig| Ok(state.clone()))
    }

    #[must_use]
    pub fn config_producer(&self) -> ConfigProducer {
        let config = self
            .state
            .config()
            .clone()
            .upgrade_for_ballista()
            .with_ballista_standalone_parallelism(1);
        Arc::new(move || config.clone())
    }

    #[must_use]
    pub fn runtime_producer(&self) -> RuntimeProducer {
        let runtime = self.state.runtime_env().clone();
        Arc::new(move |_| Ok(runtime.clone()))
    }

    #[must_use]
    pub fn function_registry(&self) -> BallistaFunctionRegistry {
        BallistaFunctionRegistry::from(&self.state)
    }
}
