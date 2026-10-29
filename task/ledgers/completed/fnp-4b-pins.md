# fnp-4b pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py, python/repark/tests/functions/test_fnp_4b_spark_dialect.py
C-002: crates/repark-core/src/idents.rs::reject_path_escape_segment, crates/repark-iceberg/src/write/idents.rs::probes, crates/repark-iceberg/src/write/idents.rs::quote_ident_spark, crates/repark-iceberg/src/write/merge/tests/lineage.rs, crates/repark-iceberg/src/write/merge/tests/merge.rs, crates/repark-spark/src/call/plan_partitioning/map.md, crates/repark-spark/src/call/run_maintenance.rs::quote_ident
C-003: crates/repark-python/src/column/expr_build.rs::parse_unresolved_expr, crates/repark-python/src/column/expr_build.rs::plan_expr_column, crates/repark-python/src/dataframe/mod.rs::drop, crates/repark-python/src/dataframe/mod.rs::filter_sql, crates/repark-python/src/dataframe/mod.rs::join_on_names, crates/repark-python/src/dataframe/mod.rs::select, crates/repark-python/src/dataframe/mod.rs::sort, crates/repark-python/src/dataframe/mod.rs::union, python/repark/tests/_acceptance.py, python/repark/tests/functions/test_fnp_4b_spark_dialect.py
C-004: crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py, python/repark/tests/functions/test_fnp_4b_literals.py
C-005: crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py, python/repark/tests/functions/test_fnp_4b_literals.py
C-006: crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py, python/repark/tests/functions/test_fnp_4b_literals.py
C-007: crates/repark-spark/src/tests/spark_dialect.rs
C-008: python/repark/src/repark/spark/_temp_views.py::local_view_name
C-009: python/repark-parity/tests/test_cap_1_source_file_line_cap.py, scripts/check_example_coverage.py
C-010: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark-parity/tests/test_cap_1_source_file_line_cap.py, python/repark/tests/_acceptance.py, scripts/check_example_coverage.py
C-011: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py
C-012: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py
C-013: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py
C-014: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py
C-015: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py
C-016: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs, python/repark/tests/_acceptance.py
C-017: crates/repark-spark/src/tests/common.rs::register_source, crates/repark-spark/src/tests/common.rs::rows, crates/repark-spark/src/tests/common.rs::run, crates/repark-spark/src/tests/common.rs::setup, crates/repark-spark/src/tests/common.rs::setup_v3_at, crates/repark-spark/src/tests/lambda_door.rs
C-018: python/repark/tests/session/test_catalog_surface.py
C-019: crates/repark-python/src/column/expr_build.rs::parse_unresolved_expr, crates/repark-python/src/column/expr_build.rs::plan_expr_column, crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral
C-020: crates/repark-spark/src/spark_literals/unescape.rs, crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral
C-021: crates/repark-python/src/column/expr_build.rs::parse_unresolved_expr, crates/repark-python/src/column/expr_build.rs::sql_context, crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral, crates/repark-spark/src/tests/spark_dialect.rs
C-022: crates/repark-spark/src/spark_typed.rs::FoldSparkNumericCasts, crates/repark-spark/src/spark_typed.rs::SparkProjectionDisplay, crates/repark-spark/src/spark_typed.rs::SuffixLiteral, python/repark/tests/_live_parity.py
C-023: crates/repark-spark/src/spark_rewrites/mod.rs, crates/repark-spark/src/tests/spark_dialect.rs
C-024: crates/repark-iceberg/src/write/merge/tests/merge_dialect.rs
C-025: crates/repark-spark/src/tests/create_format_version_one.rs, python/repark/tests/_live_parity.py
C-026: python/repark/tests/_acceptance.py, python/repark/tests/functions/test_fnp_4b_hof_display.py
C-029: python/repark/tests/_acceptance.py
