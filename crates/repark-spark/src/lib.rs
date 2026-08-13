//! repark-spark — the Spark SQL door: v1 `repark-sql` ported over the phase-1 seams.
//!
//! The statement router ([`execute`] / [`execute_with_read_only`], in [`router`]) intercepts
//! the Spark-SQL forms DataFusion cannot execute against an Iceberg catalog and passes
//! everything else through with Spark AST defaults ([`spark_ast`]). [`SparkDialect`] adapts the
//! router to the phase-1 `repark_core::SqlDialect` seam; the companion `SparkExtension`
//! (`extension` module, PR-2 WS2) installs the Spark function registry + analyzer rules.
//!
//! **PR-3b completes the port** — every v1 handler module is live (CTAS / CREATE / DROP /
//! ALTER / MERGE / INSERT OVERWRITE / CALL / ref DDL) and the router matches v1's execute
//! family end-to-end (see [`router`]).

mod alter;
mod call;
mod catalog_ops;
mod collation;
mod create_table;
mod ctas;
mod describe_show;
mod dialect;
mod insert_overwrite;
mod local_fs_ddl;
mod merge;
mod metadata_tables;
mod namespace_ddl;
mod normalize;
mod ref_ddl;
mod router;
mod spark_ast;
mod time_travel;
mod window_range;

// --- The router entrypoints (v1 `repark_sql::execute` family, re-homed). ---
pub use router::{execute, execute_with_read_only};
// G15: parse-altitude collation refuse (binding `F.expr` / `filter_sql` call this).
pub use collation::{
    COLLATION_REFUSAL_NEEDLE, collation_refusal_message, is_collation_session_key,
    refuse_collation_in_sql, refuse_collation_in_statement,
};

// --- The phase-1 seam adapter. ---
pub use dialect::SparkDialect;

// --- v1 crate-root public surface carried by the ported spine modules. ---
pub use catalog_ops::postgres_read_only_dml_message;
pub use metadata_tables::{
    canonical_metadata_table_name, is_metadata_table_name, sql_may_have_metadata_table_path,
};

// Domain-module re-exports — keep sibling `use crate::{…}` paths stable (MOVE-ONLY surface).
pub use catalog_ops::reregister_catalog_provider;
pub(crate) use catalog_ops::{
    catalog_handle, iceberg_err, name_parts, namespace_schema_name, passthrough_after_p11,
    refuse_read_only_dml_from_delete, refuse_read_only_dml_table_sql, reject_path_escape_ident,
    reregister, reregister_namespaces,
};
pub(crate) use ctas::{
    CreatePlan, build_ctas, execute_ctas, refuse_unsupported_create_table_clauses,
    resolve_create_plan_for,
};
#[cfg(test)]
pub(crate) use describe_show::{
    DescribeNamespace, describe_namespace_batch, quoted_namespace, show_namespace_rows,
};
pub(crate) use insert_overwrite::execute_insert_overwrite;
#[cfg(test)]
pub(crate) use insert_overwrite::{logical_plan_has_unsafe_cast, tighten_batch_nullability};
pub(crate) use namespace_ddl::{
    execute_create_namespace, execute_drop_namespace, execute_drop_table,
    try_parse_create_namespace,
};
pub(crate) use normalize::{
    DmlSubqueryVerb, MorDmlKind, PartitionFieldSpec, PartitionedByElement, build_partition_spec,
    build_transform_field, delete_target_object_name, object_name_from_table_with_joins,
    parse_single_normalized, property_value, refuse_dml_subquery_predicate,
    refuse_dml_subquery_predicate_in_statement, refuse_mor_unpartitioned_multi_spec_dml,
    refuse_multi_statement_sql, starts_with_branch_or_tag_ddl, starts_with_merge,
};

mod extension;
pub use extension::SparkExtension;

// The ported v1 lib-root battery (`src/tests/`, G-4 split of the former `src/tests.rs`
// monolith) reaches the v1 crate-root scope through leaf `use super::super::*`; these
// test-only imports reconstruct that scope (v1's root `use` lines + the types that moved
// to repark-core in phase 1). Shared external imports for the battery also live as
// `pub(super)` re-exports in `src/tests/common.rs`.
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use datafusion::error::DataFusionError;
#[cfg(test)]
use datafusion::prelude::SessionContext;
#[cfg(test)]
use datafusion::sql::sqlparser::ast::Statement;
#[cfg(test)]
use iceberg::Catalog;
#[cfg(test)]
use repark_core::{CatalogRegistry, LocationPolicy};

// The Q13 surface matrix: this door's disposition of every `repark_common::surfaces` ID, with
// the compile-run audit that fails on an unmapped surface (design `docs/design/sql-doors.md`
// §2 Q13, graft G2). Test-only — audit evidence, not product code.
#[cfg(test)]
mod matrix;

#[cfg(test)]
mod tests;
