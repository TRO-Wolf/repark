# Unit ledger — ICE-NESTED-EVO-1 · adopt a Spark-evolved nested table, and nested DDL (RePark half)

## Round 1 (2026-09-17) — run 21a

**Date:** 2026-09-17 · **Branch:** `fix/ice-nested-evo-1` · **Base:** `origin/main` at `2c28bec7`
(fork pin RP-24 `8fb44a39`) · **Model:** Claude Opus 5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. `risk_tier: high` (schema evolution on the Iceberg commit path).
**Registry:** `ICE-NESTED-EVO-1` **OPEN**.

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Rating row V2-10d: a Spark table whose struct gained a child
(`ALTER TABLE t ADD COLUMN s.b STRING`) is unreadable in RePark
(`Arrow Schema Error … Incorrect number of arrays for StructArray fields, expected 2 got 1`), and
nested DDL (`CREATE TABLE … (s STRUCT<…>)`, `ALTER TABLE … ADD COLUMN s.b`) is refused with no
registry row.

**Split.** The reader half is fork work — fork PR #292 (F-NESTED-EVO-1): nested struct, list and
map children matched by field id; a child the file lacks is filled from its `initial_default` or
NULL. It is not merged or pinned in this round. Pins that need it are marked
`FORK-292` below and flip green only after the orchestrator bumps the pin.

**Not in this unit:** the fork pin; `STATUS.md`; `crates/repark-iceberg/src/write/merge/`;
`crates/repark-spark/src/insert_*`.

## Oracle

`python/repark-parity/fixtures/torture/data/ice_nested_evo_1/oracle.json`, recorded 2026-09-17
by `python/repark/tests/_record_ice_nested_evo_1.py` (Spark 4.1.2 + iceberg-spark-runtime 1.11.0,
Hadoop catalog). v2 and v3 answer identically. The required-child refusal, re-recorded with the
JVM exception: `org.apache.spark.SparkException`, condition `_LEGACY_ERROR_TEMP_2045`, cause
`java.lang.IllegalArgumentException`, first line
`Unsupported table change: Incompatible change: cannot add required column: r`.

## PROPOSITION LEDGER — ICE-NESTED-EVO-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | An adopted Spark table whose struct gained `s.b` answers `SELECT id, s` as Spark does (`[(1,{a:1,b:None}),(2,{a:2,b:'y'})]`), v2 and v3, SQL and DataFrame doors. | Pin reads the committed tables after `register_table`, compares rows and Arrow types to `oracle.json`. | **OPEN** | |
| C-002 | The same tables answer the leaf projection `SELECT id, s.a, s.b` as Spark does, both doors. | Same harness. | **OPEN** | |
| C-003 | The same tables answer `WHERE s.b IS NULL` with `[1]`, both doors. | Same harness. | **OPEN** | |
| C-004 | An adopted Spark table whose list element struct gained `arrs.element.y` answers as Spark does, both doors. | Same harness on `list_add_v3`. | **OPEN** | |
| C-005 | An adopted Spark table whose map value struct gained `m.value.q` answers as Spark does, both doors. | Same harness on `map_add_v3`. | **OPEN** | |
| C-006 | `CREATE TABLE` with struct, array-of-struct and map-of-struct columns round-trips the types `DESCRIBE` shows in Spark. | Pin compares RePark `DESCRIBE` `data_type` to the oracle's. | **OPEN** | |
| C-007 | `ALTER TABLE … ADD COLUMN s.c BIGINT` on a RePark-created table adds a nullable child; the read and `DESCRIBE` match Spark. | Pin on both doors (the DataFrame door reads). | **OPEN** | |
| C-008 | `ADD COLUMN arrs.element.y INT` adds a child to the list element struct; the read matches Spark. | Same. | **OPEN** | |
| C-009 | `ADD COLUMN m.value.q STRING` adds a child to the map value struct; the read matches Spark. | Same. | **OPEN** | |
| C-010 | `RENAME COLUMN s.a TO a2` renames the child; the read matches Spark. | Same. | **OPEN** | |
| C-011 | `DROP COLUMN s.b` drops the child; the read matches Spark. | Same. | **OPEN** | |
| C-012 | `ADD COLUMN s.r INT NOT NULL` on a table with rows refuses as Spark does (message `cannot add required column: r`) and leaves the table untouched. | Pin asserts the typed exception, message and no new metadata file. | **OPEN** | |
| C-013 | Every cell above has a registry row (FIXED or DECLARED) in `docs/spark-sql-iceberg-parity.md`. | Registry section present. | **OPEN** | |

## RED — measured on main's pin `8fb44a39`, no fork override (2026-09-17)

`python/repark/tests/test_ice_nested_evo_1.py` on the release native built from `origin/main`:
**32 failed, 0 passed**.

- Adoption reads (8 cells, `fork292-*`, SQL door and DataFrame door alike):
  `PySparkException: External error: Unexpected => Arrow Schema Error, source: Invalid argument
  error: Incorrect number of arrays for StructArray fields, expected 2 got 1`.
- Nested DDL (20 cells): `CREATE TABLE … (s STRUCT<a: INT, b: STRING>)` refuses
  `UnsupportedOperationException: This feature is not implemented: column type
  `STRUCT<a INT, b STRING>` is not supported yet for Iceberg tables`; `ARRAY<STRUCT<x: INT>>`
  refuses the same way on `STRUCT<x INT>`; `MAP<STRING, STRUCT<p: INT>>` refuses at the parser
  `ParseException: SQL error: ParserError("Expected: ',' or ')' after column definition, found: <
  …")`. The dependent `DESCRIBE` and read cells then fail `TABLE_OR_VIEW_NOT_FOUND`.
- Nested ALTER, measured on its own (probe on a fresh catalog, same native):
  `ADD COLUMN s.c BIGINT` → `ParseException … Expected: a data type name, found: .`;
  `RENAME COLUMN s.a TO a2` → `ParseException … Expected: TO, found: .`;
  `DROP COLUMN s.b` → `ParseException … Expected: end of statement, found: .`.
- Required child (2 cells): `ADD COLUMN s.r INT NOT NULL` → `ParseException … Expected: a data
  type name, found: .` instead of Spark's `Unsupported table change: Incompatible change: cannot
  add required column: r`.

No registry row existed for any of these cells.

## Step 3 — the adoption reads with the fork override (2026-09-17)

Override (never committed): `.cargo/config.toml` `[patch.crates-io]` pointing the five iceberg
crates at the fork clone on `fix/nested-evo-1`. The brief's `[patch."https://github.com/TRO-Wolf/iceberg-rust"]`
header does not resolve — Cargo reports `failed to select a version for iceberg` because the
workspace's own `[patch.crates-io]` already sources the family from that git URL and a patch
does not apply to a patch source; the config-level `[patch.crates-io]` overrides the manifest's
entries (ruling Q-21a-1). `Cargo.lock` changes with the override and is never staged. Release
native built against fork `d9f226414` plus its worker's uncommitted
`nested_projection.rs` edit (the fork head moved to `c1bc78864` during the build).

Result: all eight adoption cells green on the SQL door and the DataFrame door with **no RePark
change** — the fork's field-id child matching and NULL fill is the whole reader fix.

Two RePark-side readings, both registered BACKLOG rows outside this unit's fence (ruling
Q-21a-2), measured first on the adopted `st_add_v2`:

- `SELECT id, s.a, s.b` answers Spark's values and types but names the columns
  `<table>.s[a]`, `<table>.s[b]` where Spark names them `a`, `b` — EX-COL-2 (its 2026-09-17
  note already records the SQL-door form). The pins read `s.a AS a, s.b AS b`; the unaliased
  name is the strict-xfail `test_unaliased_nested_projection_names_like_spark`.
- `df.select(col("s.a"))` / `df.filter(col("s.b").isNull())` raise `No field named s.a` — the
  same on a plain in-memory DataFrame, so not an Iceberg read defect — COL-DOTTED-FIELD-1. The
  DataFrame twins spell `col("s").getField("a").alias("a")`.

## Step 4 — nested DDL on both SQL doors (2026-09-17)

- **Engine seam** `crates/repark-iceberg/src/write/nested_column.rs`: `ColumnPathChange`
  (`Add` under an optional dotted parent with `FIRST`/`AFTER`, `Rename`, `Drop`) folded into
  ONE case-insensitive fork `UpdateSchema` by `apply_column_path_changes` —
  `add_column_to` / `add_required_column_to` with the parent path, `rename_column`,
  `delete_column`. The fork resolves `arrs.element` / `m.value` parents to the element/value
  struct. RePark keeps no schema model (fork rule 3). A sibling of `write/alter.rs` because that
  file sits at its exact file-size ceiling.
- **Spark door** `crates/repark-spark/src/nested_column_ddl.rs` (router pre-parse ahead of the
  move intercept): `ADD COLUMN[S]` (list, with or without parentheses; `NOT NULL`, `COMMENT`,
  `FIRST`/`AFTER`), `RENAME COLUMN`, `DROP COLUMN[S] [IF EXISTS]` on dotted paths, parsed with
  `SparkSqlDialect` so `MAP<K, V>` child types parse. Claims a statement only when a path has a
  dot, so every top-level form keeps its stock path.
- **CREATE TABLE** (`create_table.rs`): `STRUCT<…>` → Iceberg struct of nullable children,
  `MAP<K, V>` → Iceberg map (required key, nullable value). `normalize.rs` parses a column-def
  CREATE whose column list spells `MAP<` with `SparkSqlDialect`; every other statement keeps
  its dialect.
- **ANSI door** `crates/repark-sql/src/alter/nested.rs`: the same three forms (`GenericDialect`,
  types through the door's own `sql_type_to_iceberg`), same engine seam.

Result with the override: `test_ice_nested_evo_1.py` **46 passed, 4 xfailed** (2 × list-insert
`forkwrite`, the required-child whole-message pin, the EX-COL-2 name pin).

**Fork finding F-1 (INSERT into a list column).** On the pinned fork (and on `fix/nested-evo-1`,
the writer is byte-identical) every `INSERT` into an Iceberg list column fails:
`Unexpected => Arrow Schema Error … column types must match schema types, expected
List(Int32, field: 'element', metadata: {"PARQUET:field_id": "3"}) but found List(Int32, field:
'element')`. Backtrace: `writer/write_defaults.rs` `apply_write_defaults` (rebuilds the batch
with `schema_to_arrow_schema`, whose list element carries a field id the incoming DataFusion
batch lacks) ← `data_file_writer.rs` ← `unpartitioned_writer.rs` ← iceberg-datafusion
`task_writer.rs` / `physical_plan/write.rs`. Struct and map columns insert fine. Reproduction:
`CREATE TABLE c.ns.a (id INT, arr ARRAY<INT>) USING iceberg; INSERT INTO c.ns.a SELECT 1,
array(1, 2)`. The DataFrame `writeTo().append()` / `insertInto` / `saveAsTable(append)` fail
the same way. Pre-existing on main — not introduced here; pinned strict-xfail
(`forkwrite-…list_element_child_add_read`) and `#[ignore]` (`forkwrite_list_insert_reads_back`).

**Fork finding F-2 (required-child message).** The fork refuses a required add without a
default with `Incompatible change: cannot add required column without a default value: s.r`;
Iceberg 1.11.0 (the oracle) says `Incompatible change: cannot add required column: r` (leaf
name, no default clause). RePark also omits Spark's `Unsupported table change: ` prefix
(`SparkCatalog` wraps the Iceberg `IllegalArgumentException`), as the column-move refusals do
(ruling Q-21a-3). Pinned strict-xfail `test_required_nested_child_message_matches_spark`.
