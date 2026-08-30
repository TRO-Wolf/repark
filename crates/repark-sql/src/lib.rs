//! repark-sql — the ANSI/Trino-flavoured SQL door.

mod alter;
mod create_table;
mod dialect;
mod guards;
mod merge;
mod partitioning;
mod properties;
mod ref_ddl;
mod refusals;
mod router;
mod scan;
mod schema_ddl;
mod sniff;
mod time_travel;

// --- The seam adapter: this crate's product surface. ---
pub use dialect::AnsiDialect;

// --- The router entry point, for an embedder that holds an `EngineContext` directly. ---
pub use router::execute;

// The Q13 surface matrix records this door's disposition of every `repark_common::surfaces` ID.
#[cfg(test)]
mod matrix;

// End-to-end door tests use a native session with no extension installed.
#[cfg(test)]
mod a13_fallback;
#[cfg(test)]
mod delete_granularity;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod v3;
