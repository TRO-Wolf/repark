//! repark-spark — the Spark SQL door.

mod alter;
mod call;
mod call_args;
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
mod spark_literals;
mod time_travel;
mod window_range;

// --- Router entrypoints.
pub use router::{execute, execute_with_read_only};
// G15: parse-altitude collation refuse (binding `F.expr` / `filter_sql` call this).
pub use collation::{
    COLLATION_REFUSAL_NEEDLE, collation_refusal_message, is_collation_session_key,
    refuse_collation_in_sql, refuse_collation_in_statement,
};
pub use repark_functions::declared_refuse::{
    refuse_in_sql as refuse_declared_function_in_sql,
    refuse_in_statement as refuse_declared_function_in_statement,
};

/// Parse-altitude valves for SQL fragments that bypass the statement router.
/// # Errors
/// Collation or declared-function refusal.
pub fn refuse_sql_fragment(sql: &str) -> datafusion::error::Result<()> {
    refuse_collation_in_sql(sql)?;
    refuse_declared_function_in_sql(sql)?;
    Ok(())
}

// --- Session seam adapter.
pub use dialect::SparkDialect;
pub use repark_functions::integer_spark::install_integer_overflow;

// --- Crate-root public surface.
pub use catalog_ops::postgres_read_only_dml_message;
pub use metadata_tables::{
    canonical_metadata_table_name, is_metadata_table_name, sql_may_have_metadata_table_path,
};

// Domain-module re-exports keep sibling paths stable.
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
    refuse_multi_statement_sql, refuse_v3_cow_dml, starts_with_branch_or_tag_ddl,
    starts_with_merge,
};

mod extension;
pub use extension::SparkExtension;

// Test-only imports provide the crate-root scope shared by the leaf modules.
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

// The Q13 surface matrix.
#[cfg(test)]
mod matrix;

#[cfg(test)]
mod tests;
