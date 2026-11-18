# map — python/repark/tests/types

## Purpose

Data types and casts: the `types` module and DDL type strings, cast vocabulary and cast failures, decimal / integer / floating-point arithmetic semantics, literal typing, nullability, nested containers and logical widths.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_a3_cast_vocab.py](test_a3_cast_vocab.py) — Native cast vocabulary locksteps with ``types.py`` primitives (QUAL-03).
- [test_ansi_negative_zero_divisor_1.py](test_ansi_negative_zero_divisor_1.py) — TA-SERIES-S0b P-S0b-1 — ANSI division by negative zero matches Spark 4.1.2.
- [test_avg_decimal_round_1.py](test_avg_decimal_round_1.py) — AVG-DECIMAL-ROUND-1 (2026-11-17) — a decimal `avg` and `mean` answer Spark 4.1.2, both ANSI modes: precisions 10 to 38, grouped, `HAVING`, every window frame, `DISTINCT`, `FILTER`, `GROUPING SETS`/`ROLLUP`/`CUBE`, `UNION ALL`, a four-file merge, the DataFrame and `F.expr` doors, `describe`/`summary` means, the near-limit and overflow cells, `try_avg`, controls and every Iceberg wrong write; plus the ruled refusal of `mean(DISTINCT x), count(x)`, the bare `F.expr("mean(x)")` reaching RePark's averager (FEXPR-COUNT-STAR-AGG-1) and a two-file sum past 38 digits answering `1.0000` (Spark answers NULL or raises there); oracle [avg_decimal_round_1_spark_oracle.json](avg_decimal_round_1_spark_oracle.json).
- [_avg_decimal_round_1_cells.py](_avg_decimal_round_1_cells.py) — the cells, data and runner shared by the pin file and the oracle recorder (no `repark` import).
- [avg_decimal_round_1_spark_oracle.json](avg_decimal_round_1_spark_oracle.json) — Spark 4.1.2's answers for every cell, both ANSI modes.
- [test_decimal_divide_literal_1.py](test_decimal_divide_literal_1.py) — DECIMAL-DIVIDE-LITERAL-1 (2026-11-17) — an integer literal beside a decimal takes its digits as on Spark 4.1.2, both ANSI modes: `/` and `%` with the literal on either side, the suffixed `3S`/`3L` in `*`, `-`, `/`, `%`, `mod`/`pmod` (SQL, `F.expr`, `selectExpr`), the DataFrame `/`, `%` and `F.pmod`, the `h / 2` scopes, `avg(d / 3)`, `WHERE` and `CASE`, the Iceberg CTAS, `INSERT`, `UPDATE` and `MERGE` (copy-on-write and merge-on-read) writes, and `spark.sql.legacy.literal.pickMinimumPrecision=false`; guards (main's answer, equal to Spark's) for `d / (1 + 2)`, `d / bi`, integer and float division on both doors, written casts and `3Y`; oracle [decimal_divide_literal_1_spark_oracle.json](decimal_divide_literal_1_spark_oracle.json).
- [_decimal_divide_literal_1_cells.py](_decimal_divide_literal_1_cells.py) — the cells, data and runner shared by the pin file and the oracle recorder (no `repark` import).
- [decimal_divide_literal_1_spark_oracle.json](decimal_divide_literal_1_spark_oracle.json) — Spark 4.1.2's answers for every cell, both ANSI modes, default and `pickMinimumPrecision=false`.
- [test_bl_11_numeric_binary.py](test_bl_11_numeric_binary.py) — BL-11 numeric to BINARY under runtime ANSI — batch-17 oracle pins.
- [test_cast_failure_parity.py](test_cast_failure_parity.py) — Cast-failure semantics differential corpus (H-2 gap G6) vs live Spark 4.1.2 ANSI ON.
- [test_cast_literal_unwrap_1.py](test_cast_literal_unwrap_1.py) — CAST-LITERAL-UNWRAP-1 (2026-11-16) — a narrowing cast compared with a constant answers Spark 4.1.2 (step 0's and the critic's sets: headline cells on the SQL, DataFrame, `F.expr`, `filter(str)` and native doors; nested, `TRY_CAST`, `::`, non-ASCII, spelled-out and double-constant shapes; the four filter-moved shapes; negative halves; `DELETE` / `UPDATE` / `MERGE`, CoW and MoR), both ANSI modes; ranges plan without the shield and prune as the hand-written range; the `IN` cap; the Q2 overflow and Q4 top-of-type refusals; FLOAT, DOUBLE, STRING and timestamp-to-date controls unshielded. Evidence: `task/wo/cast-literal-unwrap-1/evidence/built/`.
- [test_decimal_common_type_1.py](test_decimal_common_type_1.py) — DECIMAL-COMMON-TYPE-1 (2026-11-16) — decimal pairs past 38 digits answer Spark 4.1.2's least common type, both ANSI modes: the position grid, constants, three types, aggregates, `VALUES`, literals, structs, map keys, `USING`, `IN` literals, set operations, the DataFrame and `F.expr` doors, every Iceberg wrong write (copy-on-write and merge-on-read) and the legacy flag; oracle [decimal_common_type_1_spark_oracle.json](decimal_common_type_1_spark_oracle.json).
- [_decimal_common_type_1_cells.py](_decimal_common_type_1_cells.py) — the cells, data and runner shared by the pin file and the oracle recorder (no `repark` import).
- [decimal_common_type_1_spark_oracle.json](decimal_common_type_1_spark_oracle.json) — Spark 4.1.2's answers for every cell, both ANSI modes.
- [test_cast_map_spell_1.py](test_cast_map_spell_1.py) — CAST-MAP-SPELL-1 — ``CAST(… AS MAP<…>)`` and ``.cast(MapType)`` answer Spark 4.1.2.
- [test_collation_refuse.py](test_collation_refuse.py) — G15 — collation refuses loudly.
- [test_decimal128_parity.py](test_decimal128_parity.py) — Decimal128 differential corpus (gap G2) + expression overflow (gap G13).
- [test_decimal_cache_1.py](test_decimal_cache_1.py) — DECIMAL-CACHE-1 pins: every oracle decimal cell on both doors and every action.
- [test_decimal_float_coercion_1.py](test_decimal_float_coercion_1.py) — WO-2 (xo-muse8 UNIT1 fix-b): decimal literals against DOUBLE/FLOAT widen the literal. Its 17 zero-bound legs answer Spark since SIGNED-ZERO-NORMALIZE-1.
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

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `_record_cast_failure_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_cast_map_spell_1.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_decimal128_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `_record_nested_container_goldens.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_cast_failure_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_decimal128_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |
| `test_nested_container_parity.py` | names its own or a sibling's new path in a docstring or recorded string. |

## Pointers

- Up: [../map.md](../map.md)
