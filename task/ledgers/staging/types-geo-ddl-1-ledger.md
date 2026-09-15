# Unit ledger — TYPES-GEO-DDL-1 · `geometry(n)` / `geography(n)` in schema strings through the Rust type table

**Date:** 2026-09-15 · **Branch:** `feat/types-geo-ddl-1` · **Base:** `origin/main`
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Card TYPES-GEO-DDL-1 (1.5 Spark-parity campaign, run 16c): `DataType.fromDDL("g geometry(4326)")`
and every spatial token raise `ValueError: cannot parse datatype`, because since FACADE-4 step 1 every DDL
token parses in the Rust type table (`crates/repark-spark/src/type_table/parse.rs::parse_atomic_token`), which
has no spatial arm. The Python type objects and their JSON round trips already answer. The recorded PySpark
4.1.2 oracle (`/tmp/oc-worker/qc-oracle/fixtures-batch13-geo.json`, cells `G13-0 … G13-20`, copied verbatim to
`python/repark/tests/fixtures-batch13-geo.json`) is the spec: `geometry(0|3857|4326|any)` parse,
`geography(4326|any)` parse, `geography(0)` / `geography(3857)` / bare tokens / unknown SRID / `-1` / CRS
strings refuse `PARSE_SYNTAX_ERROR`. The error cells recorded an empty message (Spark's ParseException text
starts with a blank line); the class is the spec and the facade's existing `ValueError: cannot parse
datatype` refusal shape stays.

**Not in this unit:** `types_bases.py` / `types.py` (run 16b's fence — untouched; the Python SRS table stays
there for the JSON door); `functions*.py` and the Python function registry (16a); `dataframe/**`, `column.py`,
`catalog.py` (16b); `STATUS.md`; a SQL-text spelling of spatial casts (the DataFusion parser owns it,
unchanged); any Arrow or engine representation of spatial values (V3-GEO-1 DECLARED stays — only the DDL
parse is in scope).

## PROPOSITION LEDGER — TYPES-GEO-DDL-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `SparkDataType::Geometry { srid }` / `Geography { srid }` variants (mixed form as Spark's `any`, stored as `-1` matching `SpatialType.MIXED_SRID`) parse in `parse_atomic_token` for exactly the oracle accept cells, refusing the rest with the existing `cannot parse datatype` `ValueError`; the supported-SRID sets live once in Rust. | `cargo test -p repark-spark type_table` arms for every cell + `test_types_geo_ddl_1.py::test_geo_ddl_parse_cells` / `::test_geo_ddl_error_cells` on both DDL doors. | **OPEN** | Red on base: `test_geo_ddl_parse_cells` fails `ValueError: cannot parse datatype: 'g geography(4326)'` (first loop cell `G13-0`); all 7 error cells already raise that `ValueError` and must keep doing so. |
| C-002 | `type_bridge.rs` carries the two tags both ways so `DataType.fromDDL`, `_parse_datatype_string`, `createDataFrame(schema="…")` parsing and `spark.read.schema("…")` parsing answer the Python objects the JSON door already builds (value AND `simpleString` AND `json` equal to the cell). | Same Python pins as C-001 (both doors share the one Rust parse) + the C-003 schema-string pins. | **OPEN** | Same red run as C-001. The Python decode arm lands in `_type_table.py::_descriptor_to_datatype` (the bridge's Python half, owned by the type-table run); `types.py` / `types_bases.py` need no edit. |
| C-003 | The Arrow mapping in `type_table.rs`: no Arrow type carries a spatial column today, so the new variants refuse `arrow_type_from_spark` loudly instead of falling into the `Utf8` catch-all; a schema string with a spatial field used to CREATE data keeps today's `UnsupportedOperationException` column-use refusal. | `test_types_geo_ddl_1.py::test_geo_ddl_create_schema_string_refuses` + `::test_geo_ddl_reader_schema_string_refuses`; existing V3-GEO-1 pins stay green. | **OPEN** | Red on base: both tests fail at the parse step (`ValueError: cannot parse datatype: 'a int, g geography(4326)'` / `'g geography(4326)'`), never reaching the refusal. Measured today: `spark_type_from_arrow` has no spatial arm because Arrow defines none; the `arrow_type_from_spark` `_` catch-all would silently answer `Utf8` for the new variants, so they get explicit refusal arms. |
| C-004 | The two BACKLOG pins flip to equality with the cell ids; registry TYPES-GEO-DDL-1 → FIXED 2026-09-15 with the pins. | `test_types_bases_1.py::test_geometry_ddl_door_blocked` + `::test_spatial_ddl_door_blocked` rewritten; `docs/spark-sql-iceberg-parity.md` row. | **OPEN** | Flip lands after the parse arm; the old refusal docstrings move to this ledger's C-001 evidence. |
| C-005 | Rust unit tests in `crates/repark-spark` for every cell; Python pins in `python/repark/tests/test_types_geo_ddl_1.py`. | `cargo test -p repark-spark type_table` + the committed Python file (red-first, committed before the fix). | **OPEN** | Python half committed red in this step's first commit; Rust half lands with the implementation. |

## Rulings

| ID | Date | Question | Ruling |
|---|---|---|---|
| Q-15B-2 | 2026-09-15 | Which run owns the spatial DDL arm? | Owner ("go with recommendations"): TYPES-GEO-DDL-1 is a 1.5 Rust card owned by the run that owns the repark-spark type table (16c). Recorded here. |

## Red evidence — 2026-09-15, base tree (`d976e34c`), release native

```
$ .venv/bin/python -m pytest python/repark/tests/test_types_geo_ddl_1.py -q
E  ValueError: cannot parse datatype: 'g geography(4326)'
FAILED test_geo_ddl_parse_cells
FAILED test_geo_ddl_create_schema_string_refuses
FAILED test_geo_ddl_reader_schema_string_refuses
3 failed, 1 passed in 0.31s
```

`test_geo_ddl_error_cells` (the 7 `PARSE_SYNTAX_ERROR` cells) passes on the base tree: the refusal shape the
fix must preserve.

VERDICT: 5 clauses, 0 PROVEN, 5 OPEN, 0 REJECTED.
