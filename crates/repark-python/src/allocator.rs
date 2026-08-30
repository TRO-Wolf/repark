//! Optional mimalloc global allocator.

/// Process-wide mimalloc allocator when the wheel's `allocator-mimalloc` feature is enabled.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
