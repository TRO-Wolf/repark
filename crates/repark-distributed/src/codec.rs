use std::sync::Arc;

use ballista_core::serde::{
    BallistaCodec, BallistaLogicalExtensionCodec, BallistaPhysicalExtensionCodec,
};

#[derive(Debug, Default)]
pub struct ReparkPhysicalExtensionCodec {
    inner: BallistaPhysicalExtensionCodec,
}

impl ReparkPhysicalExtensionCodec {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn into_inner(self) -> BallistaPhysicalExtensionCodec {
        self.inner
    }
}

#[derive(Debug, Default)]
pub struct ReparkLogicalExtensionCodec {
    inner: BallistaLogicalExtensionCodec,
}

impl ReparkLogicalExtensionCodec {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn into_inner(self) -> BallistaLogicalExtensionCodec {
        self.inner
    }
}

#[must_use]
pub fn repark_ballista_codec() -> BallistaCodec {
    BallistaCodec::new(
        Arc::new(ReparkLogicalExtensionCodec::new().into_inner()),
        Arc::new(ReparkPhysicalExtensionCodec::new().into_inner()),
    )
}
