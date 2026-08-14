//! Unit battery for the Spark SQL door (split from the former `src/tests.rs` monolith).
//!
//! Mapping rule: production-module alignment — see `task/g4-tests-split-ledger.md`.

mod common;

mod alter;
mod call;
mod catalog_ops;
mod collation;
mod create_table;
mod ctas;
mod decimal;
mod describe_show;
mod dml;
mod float_agg;
mod insert_overwrite;
mod join_null_keys;
mod local_fs_ddl;
mod merge;
mod metadata_tables;
mod namespace_ddl;
mod normalize;
mod partitioned_ctas;
mod partitioned_merge;
mod ref_ddl;
mod router;
mod service_managed_ctas;
mod time_travel;
mod transform_overwrite;
mod window_temporal_range;
