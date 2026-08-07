//! repark-spark — the Spark SQL door: v1 `repark-sql` ported over the phase-1 seams.
//!
//! The statement router ([`execute`] / [`execute_with_read_only`], in [`router`]) intercepts
//! the Spark-SQL forms DataFusion cannot execute against an Iceberg catalog and passes
//! everything else through with Spark AST defaults ([`spark_ast`]). [`SparkDialect`] adapts the
//! router to the phase-1 `repark_core::SqlDialect` seam; the companion `SparkExtension`
//! (`extension` module, PR-2 WS2) installs the Spark function registry + analyzer rules.
//!
//! **PR-2 ports the SPINE** — normalize, `spark_ast`, describe/show, metadata tables, the
//! time-travel scanner, the P11/MoR/SEC-02 guards, and the router itself. Handler modules for
//! CTAS / CREATE / DROP / ALTER / MERGE / INSERT OVERWRITE / CALL / ref DDL land in phase-2
//! PR-3a/PR-3b; their router arms refuse loudly until then (see [`router`]).

mod catalog_ops;
mod describe_show;
mod dialect;
mod local_fs_ddl;
mod metadata_tables;
mod namespace_ddl;
mod normalize;
mod router;
mod spark_ast;
mod time_travel;

// --- The router entrypoints (v1 `repark_sql::execute` family, re-homed). ---
pub use router::{execute, execute_with_read_only};

// --- The phase-1 seam adapter. ---
pub use dialect::SparkDialect;

// --- v1 crate-root public surface carried by the ported spine modules. ---
pub use catalog_ops::postgres_read_only_dml_message;
pub use metadata_tables::{
    canonical_metadata_table_name, is_metadata_table_name, sql_may_have_metadata_table_path,
};

// The v1 domain-module re-export lists (handler `pub(crate) use` groups + the lib-root test
// cohort's `#[cfg(test)]` describe/show re-exports) return with their consumers in phase-2
// PR-3a/PR-3b (the lib-root battery rides PR-3b).

mod extension;
pub use extension::SparkExtension;
