# types-geo-ddl-1 pin binding: one line per clause, read and verified by scripts/check_ledger_grammar.py rule B.

C-001: crates/repark-spark/src/type_table/tests.rs::spatial_ddl_accept_cells_parse, crates/repark-spark/src/type_table/tests.rs::spatial_srid_follows_spark_integer_value_grammar, python/repark/tests/types/test_types_geo_ddl_1.py::test_geo_ddl_parse_cells
C-002: python/repark/tests/types/test_types_geo_ddl_1.py::test_geo_ddl_bridge_tags_both_ways
C-003: crates/repark-spark/src/type_table/tests.rs::spatial_arrow_mapping_refuses_naming_the_type, crates/repark-spark/src/type_table/tests.rs::spatial_srid_follows_spark_integer_value_grammar, python/repark/tests/types/test_types_geo_ddl_1.py::test_geo_ddl_create_schema_string_refuses
C-004: python/repark/tests/types/test_types_bases_1.py::PointUdt, python/repark/tests/types/test_types_bases_1.py::test_geometry_ddl_door_answers, python/repark/tests/types/test_types_bases_1.py::test_spatial_ddl_door_answers, python/repark/tests/types/test_types_geo_ddl_1.py
C-005: crates/repark-spark/src/type_table/tests.rs::spatial_srid_follows_spark_integer_value_grammar, python/repark/tests/types/test_types_geo_ddl_1.py
