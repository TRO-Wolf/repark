//! Feature-gated mimalloc global allocator (conductor-19 AL-1a).
//!
//! Default OFF. Compiled only under `--features allocator-mimalloc`. The
//! published wheel does not enable this feature until a later AL-1b wire
//! (SQM-gated). Cargo test without the flag stays on the system allocator.
//!
//! Funding (2026-08-16): glibc malloc arena state swings the same TA query
//! ~53↔128 ns/row. Env-var tunables REGRESSED. This spike asks whether a
//! real Rust global allocator captures the fast-arena state deterministically.
//! Numerics must not change; a golden that moves is a halt, never an edit.

/// ===========================================================================================
/// Process-wide mimalloc (`#[global_allocator]`).
///
/// Lives behind `allocator-mimalloc`. Default-off; does not change the
/// published wheel until a later increment wires the feature.
/// ===========================================================================================
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
