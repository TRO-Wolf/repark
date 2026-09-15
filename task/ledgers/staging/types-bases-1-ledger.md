# Unit ledger — TYPES-BASES-1 · the abstract type bases, spatial types, `UserDefinedType`, `types.Row` — step 1

**Date:** 2026-09-14 · **Branch:** `feat/types-bases-1` · **Base:** `origin/main`
`84992add` · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card TYPES-BASES-1 (1.5 PySpark-parity campaign): `repark.spark.types`
carried only concrete classes under a flat `DataType`; PySpark 4.1.2's abstract
hierarchy (`AtomicType` → `NumericType`/`IntegralType`/`FractionalType`,
`DatetimeType`/`AnyTimeType`, `AnsiIntervalType`, `SpatialType`), the
`GeographyType`/`GeometryType` objects, `UserDefinedType`, and `types.Row` were all
absent. The run-15b oracle fixture `facade_types_oracle.json` records Spark's exact
answers for all of them.

**Not in this unit:** `DataTypeSingleton` metaclass identity (R-2 — recorded as an
out-of-scope observation, `numeric_singleton` stays false either way); any engine
execution of spatial values (V3-GEO-1 stays DECLARED); a JVM UDT registry (TYPES-UDT-1
DECLARED, same gap as `FNP-15-unwrap_udt`); `STATUS.md`, `briefs/next-sequence.md`,
dependency files, `_idents.py`, `functions*.py`, the SQL parser/planner, and
`DataFrame._quote_filter_sql_identifiers` (other runs own them tonight).

## PROPOSITION LEDGER — TYPES-BASES-1 step 1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The `isinstance_matrix` cell holds for every (type, base) pair both ways, and every recorded MRO matches. | `test_types_bases_1.py::test_isinstance_matrix_both_ways` (27 types × 10 bases), `::test_mro_matches_oracle` (16 classes), `::test_time_type_cells`. | **PROVEN** | Red on base: 27 matrix params + 15 of 16 mro params failed — `AttributeError` for the missing bases and short MROs (`Right contains 3 more items, first extra item: 'AtomicType'`). `mro[DataType]` alone was already green on the base. All 43 green after re-parenting onto `types_bases.py`; R-1 checked first — no `__bases__` reader exists anywhere in `python/` and `DataType.__eq__` compares `type(self) is type(other)` + `__dict__`, never walks the MRO. |
| C-002 | The base classes instantiate and answer Spark's repr/typeName/simpleString/json plus eq/hash. | `test_types_bases_1.py::test_base_instantiation_cells` (8 cells) + `::test_spatial_type_instantiates` + `::test_singleton_identity_not_reproduced`. | **PROVEN** | Red on base: all 8 `instantiate_*` params + `spatial_instantiate` failed `AttributeError` (`AtomicType`, `SpatialType`, …). Green after. `DataTypeSingleton` is deliberately not reproduced (R-2): `IntegralType() is IntegralType()` stays false, pinned as a divergence against oracle cell `singleton_integral`; `numeric_singleton` agrees with the oracle's false. |
| C-003 | `GeographyType`/`GeometryType` answer the oracle cells, DDL parses `geometry(4326)`, `fromJson` refuses `CANNOT_PARSE_DATATYPE`, and column use keeps the V3-GEO-1 refusal. | The geo tests + the pin appended to the V3-GEO-1 row. | **PROVEN** | Red on base: 12 geo tests failed `AttributeError: module 'repark.spark.types' has no attribute 'GeographyType'` (30 occurrences across the file). Green after: `geography_attrs`/`geometry_attrs`/`geography_any`/`geometry_any`/`geography_bad_srid` (native `IllegalArgumentException` carrying `ST_INVALID_SRID_VALUE` + `{"srid": "3857"}` via the `_integral.py` attach pattern), `geography_default`/`geometry_default` TypeErrors, `geo_ddl` (`g geometry(4326)` → `StructType([StructField('g', GeometryType(4326), True)])`), `geo_fromjson` (`PySparkValueError` `[CANNOT_PARSE_DATATYPE] Unable to parse datatype. geometry(4326).`), and `test_spatial_column_use_refuses_naming_the_type` (createDataFrame + cast refuse `UnsupportedOperationException` naming `geography(4326)`/`geometry(0)`). The vendored SRID→CRS table (`0→SRID:0`, `3857→EPSG:3857`, `4326→OGC:CRS84`, geographic flag) is copied as data in `types_bases.py`. |
| C-004 | `UserDefinedType` answers its cells and R-3's refusal is `PySparkNotImplementedError` `NOT_IMPLEMENTED` `{"feature": "UserDefinedType"}`; registry row `TYPES-UDT-1` filed. | `::test_udt_cells`, `::test_udt_column_use_refuses`, the new row at the end of §5. | **PROVEN** | Red on base: `AttributeError: module 'repark.spark.types' has no attribute 'UserDefinedType'`. Green after: `UserDefinedType()` instantiates (`typeName` `userdefinedtype`, `simpleString` `udt`), `sqlType()` raises `PySparkNotImplementedError` `[NOT_IMPLEMENTED] sqlType() is not implemented.` with `{"feature": "sqlType()"}`; a user subclass as a column type in `createDataFrame` (StructType schema) and `cast` refuses `NOT_IMPLEMENTED` `{"feature": "UserDefinedType"}` — previously the `_data_type_to_sql_type` fallback would have silently mapped the unknown type; `{"type": "udt"}` JSON refuses the same way. `TYPES-UDT-1` filed DECLARED 2026-09-14 linking `FNP-15-unwrap_udt`. |
| C-005 | No regression: `test_types*.py`, `test_row*.py`, the `-k "schema or ddl or types"` selection, and the API-inventory/cap pins stay green; `types.Row` is `spark.row.Row`. | The gate runs + `::test_types_row_is_sql_row`. | **PROVEN** | `test_types_1.py` + `test_types_bases_1.py` + `test_types_simple_string.py` + `test_types_x2_census.py` + `test_row.py`: **199 passed, 7 skipped**. `-k "schema or ddl or types"` over `python/repark/tests`: **415 passed, 18 skipped, 6087 deselected**. `repark.spark.types.Row is repark.spark.row.Row` pinned (oracle cell id `types_row_is_sql_row`, probe `cells.py:64` — the copied fixture's `types.*` subset does not carry the `row.*` cells, so the pin names the cell id from the card rather than a fixture row). Parity gate: `registry or cap_1 or map` **53 passed, 718 deselected**. |

## Per-name decisions

| Name | Disposition | Reason |
|---|---|---|
| `AtomicType` / `NumericType` / `IntegralType` / `FractionalType` / `DatetimeType` / `AnyTimeType` / `AnsiIntervalType` / `SpatialType` | implemented | Oracle `instantiate_*`/`mro`/`isinstance_matrix` cells answered byte-equal. |
| `GeographyType` / `GeometryType` | implemented (type objects) / declared (column use) | Construct, repr, simpleString, json, `srid`, `typeName`, DDL parse all Spark-equal; createDataFrame/cast/schema refuses per V3-GEO-1 (DECLARED 2026-08-25, pin appended). |
| `UserDefinedType` | declared | Class + `sqlType()`/`module()` refusals mirror Spark's own base answers; column use refuses `NOT_IMPLEMENTED` `{"feature": "UserDefinedType"}` — JVM UDT registry gap (TYPES-UDT-1, links FNP-15-unwrap_udt). |
| `types.Row` | implemented | Re-export of `repark.spark.row.Row`; identity pinned. |
| `DataTypeSingleton` metaclass | not reproduced | R-2 — identity is not part of the public contract; recorded out-of-scope (`singleton_integral` oracle cell answered `True` in Spark, repark answers `False`, pinned as a deliberate divergence). |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.
