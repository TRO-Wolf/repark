use std::sync::Arc;

use ballista_core::extension::SessionConfigExt;
use ballista_core::registry::BallistaFunctionRegistry;
use ballista_core::{ConfigProducer, RuntimeProducer};
use ballista_scheduler::SessionBuilder;
use datafusion::execution::SessionState;
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::ReparkSession;

#[derive(Clone)]
pub struct ReparkSessionProvider {
    state: SessionState,
}

impl ReparkSessionProvider {
    #[must_use]
    pub fn from_session(session: &ReparkSession) -> Self {
        Self::from_context(session.context())
    }

    #[must_use]
    pub fn from_context(context: &SessionContext) -> Self {
        Self {
            state: context.state(),
        }
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
