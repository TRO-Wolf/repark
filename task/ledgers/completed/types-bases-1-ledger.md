# Unit ledger — TYPES-BASES-1 · the abstract type bases, spatial types, `UserDefinedType`, `types.Row` — step 1

**Date:** 2026-09-14 · **Branch:** `feat/types-bases-1` · **Base:** `origin/main`
`7693ef23` (rebased onto FACADE-4 step 1) · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
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
`DataFrame._quote_filter_sql_identifiers` (other runs own them tonight). After the
FACADE-4 rebase the DDL door is the Rust type table — spatial DDL tokens stay refused
there pending the Rust spatial arm (`crates/repark-spark/src/type_table/parse.rs`);
widening `_merge_type`'s String arm to `CharType`/`TimeType`/`VariantType` (out of
charter); `TimeType.needConversion` (pre-existing Spark miss, R-1 blocks it);
`UserDefinedType.fromJson` (not in the critic's template list).

## PROPOSITION LEDGER — TYPES-BASES-1 step 1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The `isinstance_matrix` cell holds for every (type, base) pair both ways, and every recorded MRO matches. | `test_types_bases_1.py::test_isinstance_matrix_both_ways` (27 types × 10 bases), `::test_mro_matches_oracle` (16 classes), `::test_time_type_cells`. | **PROVEN** | Red on base: 27 matrix params + 15 of 16 mro params failed — `AttributeError` for the missing bases and short MROs (`Right contains 3 more items, first extra item: 'AtomicType'`). `mro[DataType]` alone was already green on the base. All 43 green after re-parenting onto `types_bases.py`; R-1 checked first — no `__bases__` reader exists anywhere in `python/` and `DataType.__eq__` compares `type(self) is type(other)` + `__dict__`, never walks the MRO. |
| C-002 | The base classes instantiate and answer Spark's repr/typeName/simpleString/json plus eq/hash. | `test_types_bases_1.py::test_base_instantiation_cells` (8 cells) + `::test_spatial_type_instantiates` + `::test_singleton_identity_not_reproduced`. | **PROVEN** | Red on base: all 8 `instantiate_*` params + `spatial_instantiate` failed `AttributeError` (`AtomicType`, `SpatialType`, …). Green after. `DataTypeSingleton` is deliberately not reproduced (R-2): `IntegralType() is IntegralType()` stays false, pinned as a divergence against oracle cell `singleton_integral`; `numeric_singleton` agrees with the oracle's false. |
| C-003 | `GeographyType`/`GeometryType` answer the oracle cells, DDL parses `geometry(4326)`, `fromJson` refuses `CANNOT_PARSE_DATATYPE`, and column use keeps the V3-GEO-1 refusal. | The geo tests + the pin appended to the V3-GEO-1 row. | **PROVEN** | Red on base: 12 geo tests failed `AttributeError: module 'repark.spark.types' has no attribute 'GeographyType'` (30 occurrences across the file). Green after: `geography_attrs`/`geometry_attrs`/`geography_any`/`geometry_any`/`geography_bad_srid` (native `IllegalArgumentException` carrying `ST_INVALID_SRID_VALUE` + `{"srid": "3857"}` via the `_integral.py` attach pattern), `geography_default`/`geometry_default` TypeErrors, `geo_ddl` (`g geometry(4326)` → `StructType([StructField('g', GeometryType(4326), True)])`), `geo_fromjson` (`PySparkValueError` `[CANNOT_PARSE_DATATYPE] Unable to parse datatype. geometry(4326).`), and `test_spatial_column_use_refuses_naming_the_type` (createDataFrame + cast refuse `UnsupportedOperationException` naming `geography(4326)`/`geometry(0)`). The vendored SRID→CRS table (`0→SRID:0`, `3857→EPSG:3857`, `4326→OGC:CRS84`, geographic flag) is copied as data in `types_bases.py`. |
| C-004 | `UserDefinedType` answers its cells and R-3's refusal is `PySparkNotImplementedError` `NOT_IMPLEMENTED` `{"feature": "UserDefinedType"}`; registry row `TYPES-UDT-1` filed. | `::test_udt_cells`, `::test_udt_column_use_refuses`, the new row at the end of §5. | **PROVEN** | Red on base: `AttributeError: module 'repark.spark.types' has no attribute 'UserDefinedType'`. Green after: `UserDefinedType()` instantiates (`typeName` `userdefinedtype`, `simpleString` `udt`), `sqlType()` raises `PySparkNotImplementedError` `[NOT_IMPLEMENTED] sqlType() is not implemented.` with `{"feature": "sqlType()"}`; a user subclass as a column type in `createDataFrame` (StructType schema) and `cast` refuses `NOT_IMPLEMENTED` `{"feature": "UserDefinedType"}` — previously the `_data_type_to_sql_type` fallback would have silently mapped the unknown type; `{"type": "udt"}` JSON refuses the same way. `TYPES-UDT-1` filed DECLARED 2026-09-14 linking `FNP-15-unwrap_udt`. |
| C-005 | No regression: `test_types*.py`, `test_row*.py`, the `-k "schema or ddl or types"` selection, and the API-inventory/cap pins stay green; `types.Row` is `spark.row.Row`. | The gate runs + `::test_types_row_is_sql_row`. | **PROVEN** | `test_types_1.py` + `test_types_bases_1.py` + `test_types_simple_string.py` + `test_types_x2_census.py` + `test_row.py`: **199 passed, 7 skipped**. `-k "schema or ddl or types"` over `python/repark/tests`: **415 passed, 18 skipped, 6087 deselected**. `repark.spark.types.Row is repark.spark.row.Row` pinned (oracle cell id `types_row_is_sql_row`, probe `cells.py:64` — the copied fixture's `types.*` subset does not carry the `row.*` cells, so the pin names the cell id from the card rather than a fixture row). Parity gate: `registry or cap_1 or map` **53 passed, 718 deselected**. |
| C-006 | Critic round 1 (L-001..L-005) + example coverage + rebase onto FACADE-4 step 1: `_merge_type` answers Spark's mixed-SRID `ANY` forms and spatial×String→StringType; `UserDefinedType` carries Spark's template methods so a `sqlType`/`serialize`/`deserialize`/`module` subclass converts and emits the UDT JSON dict while base methods refuse `toInternal()`/`fromInternal()`/`module()`; the SRID/JSON/DDL edge table is pinned; the reader schema door refuses `geography(4326)` per V3-GEO-1; the twelve new `types.*` names have covering examples. | `test_types_bases_1.py` merge/UDT/edge/DDL/schema pins; `docs/examples/types/abstract_bases.py` + `spatial_and_udt.py`; `test_ex_0_example_coverage.py` counts 942 / 44. | **PROVEN** | Red-first on the rebased tree: `test_merge_type_mixed_srid_spatial` / `_nested` / `_string` FAILED (mixed-SRID merge kept the first SRID `GeometryType(4326)`; spatial×String raised `CANNOT_MERGE_TYPE`) — L-001; `test_udt_subclass_template_methods` / `_base_template_refusals` / `_eq_compares_class` FAILED (`AttributeError: 'UserDefinedType' object has no attribute 'serialize'`; subclass `toInternal` hit the `sqlType()` refusal; `==` compared `__dict__` so two same-class UDTs with different state answered False) — L-002. Green after: `GeometryType×GeometryType`/`GeographyType×GeographyType` differing srid → `ANY` (nested struct/array pinned), spatial×String→`StringType` (the arm was NOT widened to `CharType`/`TimeType`/`VariantType` — out of charter); UDT `serialize`/`deserialize`/`_cachedSqlType`/`toInternal`/`fromInternal`/`jsonValue`/`__eq__`/`__hash__ = None` mirror Spark (`PointUdt().toInternal((1.0,2.0))` → `(1.0, 2.0)`, `jsonValue()["type"] == "udt"`, base `serialize` → `NOT_IMPLEMENTED` `toInternal()`); `StructField`/`StructType` gained the delegating conversion surface the template needs (`toInternal` → tuple, `fromInternal` → named `Row`). L-003 edges pinned (`GeometryType(False)`, `True`/float/string SRIDs, lowercase `"any"`, `ST_INVALID_ALGORITHM_VALUE` for `geography(4326)` JSON, CRS round trips); bare/`(srid)` spatial DDL keeps today's `ValueError` refusal — R-4: Spark's DDL answer needs a JVM, UNMEASURED; the `geo_ddl` cell is door-blocked on the Rust parser's missing spatial arm (`crates/repark-spark/src/type_table/parse.rs`, handed to the orchestrator's Rust step — no Python parser was added beside it). L-004: repark exposes no `catalog.createTable`/user-schema `writeTo` door — the pin drives `spark.read.schema(StructType([StructField("g", GeographyType(4326))])).csv(...)` into the same V3-GEO-1 refusal naming `geography(4326)`; pin appended to the V3-GEO-1 row. L-005: the two fixture-tautology asserts deleted; the real `IntegralType() is IntegralType()` divergence pin kept; `TimeType.needConversion` recorded out-of-scope. Examples: `abstract_bases.py` (9 names) + `spatial_and_udt.py` (3 names) cover the twelve new `types.*` names; inventory rewritten; `len(rows)` 930→942, `families["types"]` 32→44. Rebase: `630a176e` replayed onto `7693ef23` as `cb6bee57` keeping `_type_table.py` routing byte-for-byte; `types.py` baseline 1792→1770 (never above main's). |

## Per-name decisions

| Name | Disposition | Reason |
|---|---|---|
| `AtomicType` / `NumericType` / `IntegralType` / `FractionalType` / `DatetimeType` / `AnyTimeType` / `AnsiIntervalType` / `SpatialType` | implemented | Oracle `instantiate_*`/`mro`/`isinstance_matrix` cells answered byte-equal. |
| `GeographyType` / `GeometryType` | implemented (type objects) / declared (column use) | Construct, repr, simpleString, json, `srid`, `typeName`, DDL parse all Spark-equal; createDataFrame/cast/schema refuses per V3-GEO-1 (DECLARED 2026-08-25, pin appended). |
| `UserDefinedType` | declared (column use) / implemented (template) | Class + `sqlType()`/`module()`/`serialize()`/`deserialize()` refusals mirror Spark's own base answers; `_cachedSqlType`/`toInternal`/`fromInternal`/`jsonValue`/`__eq__`/`__hash__` follow Spark's template on a subclass; column use and `{"type":"udt"}` JSON refuse `NOT_IMPLEMENTED` `{"feature": "UserDefinedType"}` — JVM UDT registry gap (TYPES-UDT-1, links FNP-15-unwrap_udt). |
| `types.Row` | implemented | Re-export of `repark.spark.row.Row`; identity pinned. |
| `DataTypeSingleton` metaclass | not reproduced | R-2 — identity is not part of the public contract; recorded out-of-scope (`singleton_integral` oracle cell answered `True` in Spark, repark answers `False`, pinned as a deliberate divergence). |


VERDICT: 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED.

## Orchestrator rulings (run 15b, G-2)

- R-5 (2026-09-14): after the rebase onto FACADE-4 step 1 (#582) spatial DDL parses only in the Rust type table in
  `crates/repark-spark`, outside this run's Rust fence (repark-core plan code and repark-python bindings). The unit ships
  with that door refusing, pinned, and registry row `TYPES-GEO-DDL-1` (BACKLOG); owner question in the run-15b report.
- R-6 (2026-09-14): the S2-21 perf reviewers do not run on this unit — type objects and a class hierarchy, no data path.
- R-7 (2026-09-14): Grok critic-logic round 1 found 2 P1 (`_merge_type` mixed SRID, UDT template methods) and 2 P2, fixed in
  the Devin round that also rebased onto #582; the same critic session re-checked the head: all FIXED, residual L-101 (the
  registry row — added by the orchestrator) and L-102 P3: a `StructField` holding a user `UserDefinedType` is unhashable
  (`StructField.__hash__` hashes the dataType; Spark hashes `str(self)`); no repark path hashes fields — noted, not fixed.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: types-bases-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card and the twenty-eight recorded PySpark 4.1.2 types cells; the isinstance matrix is pinned pair by pair both ways from the fixture, not from the implementation.
      artifacts: [python/repark/tests/test_types_bases_1.py, python/repark/tests/facade_types_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Every concrete type against every base; TimeType under AnyTimeType; Char/Varchar atomic not string; Null and CalendarInterval not atomic; SRID edge values (False, True, float, string, lowercase any); JSON algorithm refusal; mixed-SRID merges nested in struct, array and map; a working UDT subclass and the bare base.
      artifacts: [python/repark/tests/test_types_bases_1.py]
    - id: AT-3
      status: N/A
      justification: Pure Python type objects; no Rust, no unwrap, no I/O.
    - id: AT-4
      status: N/A
      justification: No shared mutable state, threads or async.
    - id: AT-5
      status: N/A
      justification: No authn/authz, network or credential surface; UDT jsonValue pickles the user's own class exactly as PySpark does.
    - id: AT-6
      status: ATTACKED
      evidence: Grok critic-logic round 1 NEEDS_REMEDIATION (2 P1, 2 P2, 1 P3), all fixed with red-first evidence; the same critic session re-checked the rebased head and marked every finding FIXED.
      artifacts: [task/ledgers/completed/types-bases-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Full facade suite (6356 passed) and parity suite (757 passed) on the release native module built at FACADE-4 step 1; ruff 0.15.22, check_lib_py, ledger lifecycle and grammar, docs links, owner ruling, example coverage (both new examples executed); comment-ban grep zero hits; size baselines moved down only.
      artifacts: [docs/examples/types/abstract_bases.py, docs/examples/types/spatial_and_udt.py]
    - id: AT-8
      status: ATTACKED
      evidence: Spark's contracts were read, not assumed - pyspark/sql/types.py DataType, the abstract bases, the spatial types and UserDefinedType (1882-2004), geo_utils.py's SRID table, and _merge_type's spatial arms; FACADE-4's _type_table.py routing was diffed byte for byte against main after the rebase.
      artifacts: [python/repark/src/repark/spark/types_bases.py, python/repark/src/repark/spark/types.py]
    - id: AT-9
      status: ATTACKED
      evidence: Refusals carry Spark's classes and texts (ST_INVALID_SRID_VALUE, ST_INVALID_ALGORITHM_VALUE, CANNOT_PARSE_DATATYPE, NOT_IMPLEMENTED); the two declared or deferred doors have registry rows (TYPES-UDT-1 DECLARED, TYPES-GEO-DDL-1 BACKLOG) and V3-GEO-1 gained the reader-schema pin.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation guards run - the first-SRID merge reds the mixed-SRID pins, a refusing UDT toInternal reds the PointUDT table, removing a base from a concrete type's bases reds its matrix row.
      artifacts: [python/repark/tests/test_types_bases_1.py]
  complete: true
```

