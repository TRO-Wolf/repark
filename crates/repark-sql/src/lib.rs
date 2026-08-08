//! repark-sql — the ANSI/Trino-flavoured SQL door.
//!
//! NEW code (not a port): [`AnsiDialect`] implements the frozen phase-1
//! [`repark_core::SqlDialect`] seam and routes one statement at a time. The door delegates to
//! stock DataFusion wherever DataFusion is already right — reads, `information_schema`, temp
//! views, and the fork `TableProvider`'s `DELETE`/`UPDATE`/`INSERT` (ADR-0003) — and intercepts
//! only the Iceberg catalog DDL DataFusion cannot express.
//!
//! Design SSOT: `docs/design/sql-doors.md` §2 (rulings Q1–Q15). The shape in one line: guards run
//! FIRST (multi-statement, read-only-catalog DML, write-to-branch), then metadata passthrough,
//! then a stock parse, then the statement match, with the local-filesystem guard between planning
//! and execution on the delegation path; the wrong-door sniff runs on the ERROR path only, so the
//! happy path pays nothing for it and string literals can never trigger it.
//!
//! Milestone scope (PR-5 / M1): the crate spine, the guard set, the wrong-door sniff, the
//! `CREATE TABLE` family with its `WITH (…)` vocabulary and Q15 routing, `CREATE`/`DROP SCHEMA`,
//! and `DROP TABLE`. `MERGE`, `ALTER`, `FOR … AS OF` time travel and branch/tag DDL land in PR-6
//! — each recorded as a typed absence row in the surface matrix, never as silence.

mod create_table;
mod dialect;
mod guards;
mod partitioning;
mod properties;
mod router;
mod scan;
mod schema_ddl;
mod sniff;

// --- The phase-1 seam adapter: this crate's product surface. ---
pub use dialect::AnsiDialect;

// --- The router entry point, for an embedder that holds an `EngineContext` directly. ---
pub use router::execute;

// The Q13 surface matrix: this door's disposition of every `repark_common::surfaces` ID, with
// the compile-run audit that fails on an unmapped surface (design `docs/design/sql-doors.md`
// §2 Q13, graft G2). Test-only — audit evidence, not product code.
#[cfg(test)]
mod matrix;

// End-to-end door tests on a NATIVE session (no extension installed) — the profile every ANSI
// matrix row claims. File-backed per the crate-root thinness guard.
#[cfg(test)]
mod tests;
