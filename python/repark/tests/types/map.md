# map — python/repark/tests/types

## Purpose

Data types and casts: the `types` module and DDL type strings, cast vocabulary and cast failures, decimal / integer / floating-point arithmetic semantics, literal typing, nullability, nested containers and logical widths.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_a3_cast_vocab.py](test_a3_cast_vocab.py) — Native cast vocabulary locksteps with ``types.py`` primitives (QUAL-03).
- [test_ansi_negative_zero_divisor_1.py](test_ansi_negative_zero_divisor_1.py) — TA-SERIES-S0b P-S0b-1 — ANSI division by negative zero matches Spark 4.1.2.
- [test_bl_11_numeric_binary.py](test_bl_11_numeric_binary.py) — BL-11 numeric to BINARY under runtime ANSI — batch-17 oracle pins.
- [test_cast_failure_parity.py](test_cast_failure_parity.py) — Cast-failure semantics differential corpus (H-2 gap G6) vs live Spark 4.1.2 ANSI ON.
- [test_cast_map_spell_1.py](test_cast_map_spell_1.py) — CAST-MAP-SPELL-1 — ``CAST(… AS MAP<…>)`` and ``.cast(MapType)`` answer Spark 4.1.2.
- [test_collation_refuse.py](test_collation_refuse.py) — G15 — collation refuses loudly.
- [test_decimal128_parity.py](test_decimal128_parity.py) — Decimal128 differential corpus (gap G2) + expression overflow (gap G13).
- [test_decimal_cache_1.py](test_decimal_cache_1.py) — DECIMAL-CACHE-1 pins: every oracle decimal cell on both doors and every action.
- [test_decimal_float_coercion_1.py](test_decimal_float_coercion_1.py) — WO-2 (xo-muse8 UNIT1 fix-b): decimal literals against DOUBLE/FLOAT widen the literal.
- [test_facade_4_census_pins.py](test_facade_4_census_pins.py) — FACADE-4 step-1 census pins: every Agree and D1-D24 row's measured answer.
- [test_facade_4_ddl_round_trip.py](test_facade_4_ddl_round_trip.py) — FACADE-4 type-conversion goldens against the committed JSON file.
- [test_facade_4_step1_remediation.py](test_facade_4_step1_remediation.py) — FACADE-4 step-1 remediation pins: byte-identity repros for the round-2 findings.
- [test_intdiv_1.py](test_intdiv_1.py) — INTDIV-1: integer division scopes and the `div` operator replay Spark on both doors.
- [test_integer_overflow_parity.py](test_integer_overflow_parity.py) — Integer `+` / `-` / `*` overflow pins (F-Y10-1).
- [test_java_double_fd_1.py](test_java_double_fd_1.py) — JAVA-DOUBLE-FD-1 pins: JDK-longhand text, HALF_UP %f, suffixed casts.
- [test_java_double_str_1.py](test_java_double_str_1.py) — Spark-door DOUBLE/FLOAT stringify answers Java Double.toString (BL-7 FIXED).
- [test_logical_width_1.py](test_logical_width_1.py) — LOGICAL-WIDTH-1 — Spark's logical widths on the facade, both doors.
- [test_n1_nested_dict_struct.py](test_n1_nested_dict_struct.py) — ``spark.sql.pyspark.inferNestedDictAsStruct.enabled`` (SPARK-35929).
- [test_nested_container_parity.py](test_nested_container_parity.py) — Nested-container differential corpus — list / struct / map vs live Spark.
- [test_nullability_2.py](test_nullability_2.py) — The analyzer's remaining nullability and cast residues, Spark-equal.
- [test_sql_literal_typing_1.py](test_sql_literal_typing_1.py) — BL-20 pins: Spark integral literal typing and binary-arithmetic promotion.
- [test_types_1.py](test_types_1.py) — TYPES-1 — Spark-door Arrow types: literals, count-likes, rank, from_unixtime.
- [test_types_bases_1.py](test_types_bases_1.py) — TYPES-BASES-1: abstract type bases, spatial types, ``UserDefinedType``, ``types.Row``.
- [test_types_geo_ddl_1.py](test_types_geo_ddl_1.py) — TYPES-GEO-DDL-1: ``geometry(n)`` / ``geography(n)`` in DDL schema strings.
- [test_types_simple_string.py](test_types_simple_string.py) — R-PARITY-NITS / X2 — DataType.simpleString / typeName / json / fromDDL / StructType.add.
- [test_types_x2_census.py](test_types_x2_census.py) — X2 / R-CENSUS-TYPES — Row repr factory + createDataFrame nested / LongType schema.
- [test_uuid_cast_window_1.py](test_uuid_cast_window_1.py) — WO UUID-CAST-WINDOW-1: the UUID refusal's SQL window counts as Spark counts.
- [_record_cast_failure_goldens.py](_record_cast_failure_goldens.py) — Record mode for the cast-failure corpus — re-derive every Spark half from live PySpark.
- [_record_cast_map_spell_1.py](_record_cast_map_spell_1.py) — Re-derive the CAST-MAP-SPELL-1 Spark oracle from live PySpark 4.1.2.
- [_record_decimal128_goldens.py](_record_decimal128_goldens.py) — Record mode for the decimal128 corpus — re-derive every `spark` half from live PySpark.
- [_record_nested_container_goldens.py](_record_nested_container_goldens.py) — Record mode for the nested-container corpus — re-derive every Spark half from live PySpark.
- [cast_map_spell_1](cast_map_spell_1/map.md) — CAST-MAP-SPELL-1 oracle recordings (own `map.md`).
- [decimal_cache_1_oracle.json](decimal_cache_1_oracle.json) — The 30 live-PySpark oracle cells (six input types under five decimal expressions) driving `test_decimal_cache_1.py`; recorded evidence, never hand-edited.
- [decimal_float_coercion_1_spark_oracle.json](decimal_float_coercion_1_spark_oracle.json) — Recorded Spark oracle (64 cells) read by `test_decimal_float_coercion_1.py`.
- [facade_4_type_goldens.json](facade_4_type_goldens.json) — Golden file read by `test_facade_4_ddl_round_trip.py`.
- [facade_logical_width_oracle.json](facade_logical_width_oracle.json) — Recorded Spark oracle read by `test_logical_width_1.py`.
- [facade_types_oracle.json](facade_types_oracle.json) — Recorded Spark oracle (28 cells) read by `test_types_bases_1.py`.
- [fixtures-batch13-geo.json](fixtures-batch13-geo.json) — Recorded data read by `test_types_geo_ddl_1.py`.
- [intdiv_1_spark_oracle.json](intdiv_1_spark_oracle.json) — Recorded Spark oracle (363 cells) read by `test_intdiv_1.py`.
- [sql_literal_typing_1_spark_oracle.json](sql_literal_typing_1_spark_oracle.json) — Recorded Spark oracle (77 cells) read by `test_sql_literal_typing_1.py`.
- [uuid_cast_window_1_spark_oracle.json](uuid_cast_window_1_spark_oracle.json) — Recorded Spark oracle read by `test_uuid_cast_window_1.py`.

## Pointers

- Up: [../map.md](../map.md)
