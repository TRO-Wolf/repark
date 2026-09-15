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
| C-001 | `SparkDataType::Geometry { srid }` / `Geography { srid }` variants (mixed form as Spark's `any`, stored as `-1` matching `SpatialType.MIXED_SRID`) parse in `parse_atomic_token` for exactly the oracle accept cells, refusing the rest with the existing `cannot parse datatype` `ValueError`; the supported-SRID sets live once in Rust. | `cargo test -p repark-spark type_table` arms for every cell + `test_types_geo_ddl_1.py::test_geo_ddl_parse_cells` / `::test_geo_ddl_error_cells` on both DDL doors. | **PROVEN** | Red-first in `a7330a74` (`ValueError: cannot parse datatype: 'g geography(4326)'`). Green after: `type_table::tests::spatial_ddl_accept_cells_parse` + `::spatial_ddl_refuse_cells_keep_parse_error` (4/4 in the `type_table` filter run) and `test_geo_ddl_parse_cells` / `::test_geo_ddl_error_cells` on `DataType.fromDDL` and `_parse_datatype_string` (value, `simpleString`, `json`, `repr` equal to the cell; the 7 `PARSE_SYNTAX_ERROR` cells keep the `ValueError` shape). |
| C-002 | `type_bridge.rs` carries the two tags both ways so `DataType.fromDDL`, `_parse_datatype_string`, `createDataFrame(schema="…")` parsing and `spark.read.schema("…")` parsing answer the Python objects the JSON door already builds (value AND `simpleString` AND `json` equal to the cell). | Same Python pins as C-001 (both doors share the one Rust parse) + the C-003 schema-string pins. | **PROVEN** | `test_geo_ddl_bridge_tags_both_ways` pins the wire shape directly (`simple_string_from_descriptor({"kind": "geometry", "srid": 4326})` → `"geometry(4326)"`, `geography/-1` → `"geography(any)"`, `ddl_token_from_descriptor(geometry/0)` → `"GEOMETRY(0)"`). The Python decode arm landed in `_type_table.py::_descriptor_to_datatype` (the bridge's Python half); no encoder arm was added, so every other surface keeps its fallback/refusal bytes; `types.py` / `types_bases.py` untouched. |
| C-003 | The Arrow mapping in `type_table.rs`: no Arrow type carries a spatial column today, so the new variants refuse `arrow_type_from_spark` loudly instead of falling into the `Utf8` catch-all; a schema string with a spatial field used to CREATE data keeps today's `UnsupportedOperationException` column-use refusal. | `test_types_geo_ddl_1.py::test_geo_ddl_create_schema_string_refuses` + `::test_geo_ddl_reader_schema_string_refuses`; existing V3-GEO-1 pins stay green. | **PROVEN** | Red-first in `a7330a74` (both tests died at the parse step). Green after: `spatial_arrow_mapping_refuses_naming_the_type` plus both Python refusal tests (`createDataFrame([], "a int, g geography(4326)")` and `read.schema("g geography(4326)").csv().collect()` refuse `UnsupportedOperationException` naming `geography(4326)`). Measured: Arrow defines no spatial type (`spark_type_from_arrow` correctly has no arm); the new `arrow_type_from_spark` arms refuse instead of the silent `Utf8` catch-all. Existing V3-GEO-1 pins green (`test_types_bases_1.py` 77 passed; only the two C-004 BACKLOG pins red). |
| C-004 | The two BACKLOG pins flip to equality with the cell ids; registry TYPES-GEO-DDL-1 → FIXED 2026-09-15 with the pins. | `test_types_bases_1.py::test_geometry_ddl_door_answers` + `::test_spatial_ddl_door_answers`; `docs/spark-sql-iceberg-parity.md` row. | **PROVEN** | Renamed (not just re-asserted — `door_blocked` would lie) and green: `geo_ddl` repr plus struct equality; three parses plus the two surviving bare-token refusals. The pins reddened on the implementation tree before the flip (2 failed, 77 passed) and pass after. Registry row rewritten FIXED 2026-09-15 with the full pin list. |
| C-005 | Rust unit tests in `crates/repark-spark` for every cell; Python pins in `python/repark/tests/test_types_geo_ddl_1.py`. | `cargo test -p repark-spark type_table` + the committed Python file (red-first, committed before the fix). | **PROVEN** | Python half committed red in `a7330a74`; Rust half (`type_table/tests.rs`, file-backed `#[cfg(test)]` module, 4 tests over all 21 cells) green in the `type_table` filter run; Python file 5/5 on the rebuilt release native. |

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

## Review round 2 — 2026-09-15 (Grok critic-logic + S2-21 Rust perf, PR #624)

| Finding | Reviewer | Severity | Disposition |
|---|---|---|---|
| L-001: the SRID capture was Python-`int`, not Spark's `INTEGER_VALUE` (`4_326`, `+4326`, fullwidth digits parsed). | critic-logic | P2 | Fixed: the capture is ASCII `[0-9]+`, same shape as the `decimal`/`char` arms; leading zeros still parse. Pinned in `spatial_srid_follows_spark_integer_value_grammar` and `test_geo_ddl_integer_value_grammar_edges`. The three refusals rest on Spark grammar knowledge, not a live cell — said in the pin docstrings, not in a comment. |
| L-002: tab/NBSP inside the parens diverted to the Python residue (no spatial arm) and refused. | critic-logic | P2 | Fixed: the `isprintable` divert narrows by spatial keyword in one line, so these strings reach the Rust table, whose trim matches `str.strip`. One-line Python reason: the divert lives in Python and only Python can narrow it; Rust already accepted the tabs. Pinned in `test_geo_ddl_surrounding_whitespace_doors` with the decimal tab control. `_type_table.py` net line change is zero. |
| L-003: `spark_type_from_py` accepted any SRID (`geography` + `0` built). | critic-logic | P3 | Fixed: `spatial_srid_supported` in the table plus a `validated_spatial_srid` bridge helper refuse with `ValueError` naming the SRID. Pinned in the bridge-tags test. Reachable only through hand-built dicts (no Python encoder emits spatial descriptors). |
| L-004: `sql_type_from_token` still maps spatial tokens to a Utf8 string capsule, as on main. | critic-logic | P3 | Recorded; no change, not a regression. |
| P3-1: miss tokens run two extra regex captures before the refusal. | S2-21 perf | P3 | Declined with reason: the suggested `starts_with` gate does not stay a two-line change once formatted, and miss tokens pay ~0.2 µs on a cold path common tokens never reach. |

Round-2 notes. A live-oracle re-measure is out of lane (no JVM here); the recorded fixture is the spec, and the full G13 suite stays green, so no oracle drift. The grammar edges above are disclosed as grammar-knowledge, not live cells. One release-native rebuild covers round 2 (the round-1 red run already proved the rebased baseline, and `cargo check` passed on it).

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: types-geo-ddl-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause walked against the card and the twenty-one recorded PySpark 4.1.2 DDL cells; both DDL doors (`DataType.fromDDL`, `_parse_datatype_string`) assert value, simpleString and json per accept cell, not one representative case.
      artifacts: [python/repark/tests/test_types_geo_ddl_1.py, python/repark/tests/fixtures-batch13-geo.json]
    - id: AT-2
      status: ATTACKED
      evidence: Refusal cells (geography(0/3857), bare tokens, unknown SRID, -1, CRS string), case variants (`GEOMETRY`, `ANY`), inner whitespace, nesting in struct/array/map/field-list, the mixed `any` marker, and the still-refusing CREATE/reader schema-string doors.
      artifacts: [python/repark/tests/test_types_geo_ddl_1.py, crates/repark-spark/src/type_table/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: No unwrap/expect in product code (the new arms use let-else and combinators); test-only unwrap covered by `allow-unwrap-in-tests`; `rust-panic-ban` runs in the gate set.
      artifacts: [crates/repark-spark/src/type_table/parse.rs, crates/repark-spark/src/type_table.rs]
    - id: AT-4
      status: N/A
      justification: Pure parse over immutable input; the only shared state is the pre-existing process-local regex cache.
    - id: AT-5
      status: N/A
      justification: No authn/authz, network or credential surface; DDL text in, descriptor out.
    - id: AT-6
      status: N/A
      justification: No delegated critic round in this single-round lane; the adversarial check is the red-first pins (failed before, pass after) plus the BACKLOG pins reddening on the implementation tree before their flip.
    - id: AT-7
      status: ATTACKED
      evidence: `cargo test -p repark-spark type_table`, `cargo test -p repark-python --lib`, `make verify`, the facade suite and the `-k "ddl or types or schema"` selection, all green on the release native; ruff 0.15.22 clean; comment-ban grep empty on every commit.
      artifacts: [python/repark/tests/test_types_geo_ddl_1.py, python/repark/tests/test_types_bases_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: The oracle fixture is a verbatim copy of the recorded PySpark 4.1.2 cells (`cmp` identical); error-cell messages were never asserted beyond the class-shaped refusal the facade already owns, per the card.
      artifacts: [python/repark/tests/fixtures-batch13-geo.json]
    - id: AT-9
      status: ATTACKED
      evidence: Accept cells answer Spark's value and type; refusal cells keep `ValueError: cannot parse datatype`; column use keeps `UnsupportedOperationException` naming the type (V3-GEO-1); the Arrow mapping refuses naming the type instead of silent `Utf8`.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first mutation proof — the new pins failed on the base tree (`cannot parse datatype: 'g geography(4326)'`) and pass after; the flipped BACKLOG pins failed on the implementation tree before the flip and pass after.
      artifacts: [python/repark/tests/test_types_geo_ddl_1.py, python/repark/tests/test_types_bases_1.py]
  complete: true
```
