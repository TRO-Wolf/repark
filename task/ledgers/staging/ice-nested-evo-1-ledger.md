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
| C-001 | An adopted Spark table whose struct gained `s.b` answers `SELECT id, s` as Spark does (`[(1,{a:1,b:None}),(2,{a:2,b:'y'})]`), v2 and v3, SQL and DataFrame doors. | Pin reads the committed tables after `register_table`, compares rows and Arrow types to `oracle.json`. | **PROVEN** | `test_adopted_spark_table_reads_match_spark[fork292-v{2,3}_struct_child_add_read-…]` green on both doors with the fork override; red at pin `8fb44a39` (arity error) until the pin bump. |
| C-002 | The same tables answer the leaf projection `SELECT id, s.a, s.b` as Spark does, both doors. | Same harness. | **PROVEN** | `…[fork292-v{2,3}_struct_child_add_leaf_read-…]` green (values and types; read aliased `s.a AS a` — the unaliased name is EX-COL-2, strict-xfail `test_unaliased_nested_projection_names_like_spark`). |
| C-003 | The same tables answer `WHERE s.b IS NULL` with `[1]`, both doors. | Same harness. | **PROVEN** | `…[fork292-v{2,3}_struct_child_add_filter_null-…]` green, both doors (`getField("b").isNull()` twin). |
| C-004 | An adopted Spark table whose list element struct gained `arrs.element.y` answers as Spark does, both doors. | Same harness on `list_add_v3`. | **PROVEN** | `…[fork292-v3_list_element_child_add_read-list_add_v3]` green, both doors. |
| C-005 | An adopted Spark table whose map value struct gained `m.value.q` answers as Spark does, both doors. | Same harness on `map_add_v3`. | **PROVEN** | `…[fork292-v3_map_value_child_add_read-map_add_v3]` green, both doors. |
| C-006 | `CREATE TABLE` with struct, array-of-struct and map-of-struct columns round-trips the types `DESCRIBE` shows in Spark. | Pin compares RePark `DESCRIBE` `data_type` to the oracle's. | **PROVEN** | `test_nested_ddl_schema_matches_spark[{nested_create,list_element_child_add,map_value_child_add}_describe-v{2,3}]` green; Rust `nested_create_round_trips_the_describe_types`. |
| C-007 | `ALTER TABLE … ADD COLUMN s.c BIGINT` on a RePark-created table adds a nullable child; the read and `DESCRIBE` match Spark. | Pin on both doors (the DataFrame door reads). | **PROVEN** | `test_nested_ddl_schema_matches_spark[nested_create_then_add-v{2,3}]` + `test_nested_ddl_rows_match_spark[fork292-nested_create_then_add-v{2,3}]` + `struct_child_add_*` rows green (override); ANSI door `nested_add_rename_and_drop_on_the_ansi_door`. |
| C-008 | `ADD COLUMN arrs.element.y INT` adds a child to the list element struct; the read matches Spark. | Same. | **OPEN** | Schema half PROVEN: `test_nested_ddl_schema_matches_spark[list_element_child_add_read-v{2,3}]` and the `_describe` cells green. Rows half blocked: an `INSERT` into a list column fails in the fork writer (fork finding F-1, registry ICE-NESTED-INSERT-LIST-1), strict-xfail `forkwrite-…list_element_child_add_read`. Closes when the fork writer takes list inserts. |
| C-009 | `ADD COLUMN m.value.q STRING` adds a child to the map value struct; the read matches Spark. | Same. | **PROVEN** | `test_nested_ddl_schema_matches_spark[map_value_child_add_*]` and `test_nested_ddl_rows_match_spark[fork292-map_value_child_add_read-v{2,3}]` green (override). |
| C-010 | `RENAME COLUMN s.a TO a2` renames the child; the read matches Spark. | Same. | **PROVEN** | `test_nested_ddl_*[nested_rename_read-v{2,3}]` green; Rust pins keep the field id (`nested_add_rename_and_drop_evolve_by_field_id`, seam `column_path_changes_evolve_nested_children_by_field_id`); ANSI door pin. |
| C-011 | `DROP COLUMN s.b` drops the child; the read matches Spark. | Same. | **PROVEN** | `test_nested_ddl_*[nested_drop_read-v{2,3}]` green; Rust + ANSI door pins. |
| C-012 | `ADD COLUMN s.r INT NOT NULL` on a table with rows refuses as Spark does (message `cannot add required column: r`) and leaves the table untouched. | Pin asserts the typed exception, message and no new metadata file. | **PROVEN** | Round 2 (2026-09-18): Spark's whole first line `Unsupported table change: Incompatible change: cannot add required column: r` on both doors — `test_required_nested_child_message_matches_spark[2,3]` (strict xfail dropped), `test_required_nested_child_refuses_like_spark[2,3]`, ANSI twin `ansi_nested_ddl_oracle.rs` (whole line), seam `nested_add_refusal_answers_spark_for_known_and_unknown_paths`; RePark-side `nested_required_add_refusal` (Q-22b-NEST-4). Round 1: refusal proven, message tail was the fork's. |
| C-013 | Every cell above has a registry row (FIXED or DECLARED) in `docs/spark-sql-iceberg-parity.md`. | Registry section present. | **PROVEN** | `docs/spark-sql-iceberg-parity.md` §7: ICE-NESTED-EVO-1 FIXED, ICE-NESTED-DDL-1 FIXED (round-2 paragraph), ICE-NESTED-DDL-1-R-001 FIXED 2026-09-18, R-002 … R-005 FIXED 2026-09-18, ICE-NESTED-INSERT-LIST-1 OPEN; `docs/map.md` index lines. |
| C-014 | (L-001) Nested `CREATE TABLE` writes Spark's field ids — Java's level order (`id`=1, `s`=2, `arr`=3, `m`=4, `s.a`=5, `s.b`=6, `s.b.c`=7, `arr.element`=8, `x`=9, `m.key`=10, `m.value`=11, `q`=12) — on the Spark SQL door, the ANSI door and the DataFrame door. | Pins compare the metadata file's current schema to Spark's recorded metadata. | **PROVEN** | `test_nested_ddl_metadata_matches_spark[create_field_ids-v{2,3}]`, `test_dataframe_door_create_matches_spark_metadata[create_field_ids-v{2,3}]`, ANSI `every_recorded_nested_schema_cell_answers_spark_on_the_ansi_door`, Rust `nested_create_not_null_child_is_required_with_level_order_ids`. No code change: the fork's `TableMetadataBuilder::new` reassigns (Q-22b-NEST-1). |
| C-015 | (L-002 / L-006) A backtick dotted name (`` TO `x.y` ``, `` s.`x.y` ``, `` `p.q` ``) is one leaf named `x.y`, as Spark; the double-quoted spelling refuses `PARSE_SYNTAX_ERROR` near `'"x.y"'` 42601 with the schema untouched, as Spark. | Pins on the facade (metadata, refusal class and text); ANSI twin. | **PROVEN** | `test_nested_ddl_{metadata,outcome,read_schema}_matches_spark[{rename_dotted,add_dotted_leaf,add_dotted_top,rename_double_quoted,add_double_quoted_leaf}-v{2,3}]`, Rust `nested_ddl_refuses_double_quoted_names_and_known_paths_spark_shaped`, ANSI twin (Q-22b-NEST-3). Registry R-002. |
| C-016 | (L-003) `CREATE TABLE … (s STRUCT<a: INT NOT NULL, b: STRING>)` creates `s.a` required (Spark's metadata), both SQL doors; the read answers `a` non-nullable; the DataFrame-door create answers Spark's all-optional schema. | Metadata, read-shape and DataFrame-door pins. | **PROVEN** | `test_nested_ddl_*[create_required_child-v{2,3}]`, `test_dataframe_door_create_matches_spark_metadata[create_required_child-v{2,3}]`, Rust `nested_create_not_null_child_is_required_with_level_order_ids`, seam `nested_type_sql` tests, ANSI twin. Registry R-003. |
| C-017 | (L-004) The ANSI door answers every recorded nested DDL cell (reads, `DESCRIBE`, metadata, refusals), v2 and v3 — `MAP<…>` in CREATE and ADD COLUMN, `element` / `value` paths included. | Oracle-driven Rust twin of every cell. | **PROVEN** | `crates/repark-sql/tests/ansi_nested_ddl_oracle.rs` (2 tests; red before: 33 cells). Registry R-004. |
| C-018 | (L-005) `ADD COLUMN s.z INT FIRST`, `ADD COLUMN s.w INT AFTER a`, `ADD COLUMN s.d INT COMMENT 'c'` leave Spark's child order, ids and `doc` in the metadata; `AFTER s.a` refuses `PARSE_SYNTAX_ERROR` 42601, schema untouched; both doors. | Metadata and refusal pins. | **PROVEN** | `test_nested_ddl_{metadata,outcome}_matches_spark[{add_first,add_after,add_comment,add_after_dotted}-v{2,3}]`, ANSI twin. No code change. |
| C-019 | (item 6) `ADD COLUMN s.a INT` on an existing child refuses `AnalysisException` `FIELD_ALREADY_EXISTS` 42710 and `ADD COLUMN nope.z INT` refuses `AnalysisException` `UNRESOLVED_COLUMN.WITH_SUGGESTION` 42703 with Spark's text, schema untouched, both doors. | Refusal pins (class, text up to `SQLSTATE`, metadata). | **PROVEN** | `test_nested_ddl_{metadata,outcome}_matches_spark[{add_duplicate_child,add_unknown_parent}-v{2,3}]`, Rust `nested_ddl_refuses_double_quoted_names_and_known_paths_spark_shaped`, seam `nested_add_refusal_answers_spark_for_known_and_unknown_paths`, ANSI twin. Registry R-005. |
| C-020 | `ADD COLUMN s.mm MAP<STRING, INT>`, `s.st STRUCT<u: INT, v: STRUCT<w: INT>>`, `s.al ARRAY<STRUCT<k: INT>>` give Spark's child types and ids (level order from the next free id), both doors. | Metadata pins. | **PROVEN** | `test_nested_ddl_metadata_matches_spark[{add_map_child,add_struct_child,add_list_child}-v{2,3}]`, ANSI twin. |
| C-021 | (V-001, round 3) A double-quoted token in a string position is Spark's string literal: `ADD COLUMN s.d INT COMMENT "x.y"`, `s.e INT COMMENT "c"`, `s.f INT COMMENT "x.y" FIRST` succeed with `doc` `x.y` / `c` and `f` first; the double-quoted refusal holds only in a name position (C-015 unchanged). | Metadata, outcome and read-shape pins on the facade Spark door; ANSI twin; Rust Spark-door pin. | **PROVEN** | `test_nested_ddl_{metadata,outcome,read_schema}_matches_spark[add_comment_double_quoted{,_dotted,_first}-v{2,3}]` (18, red before), Rust `nested_add_comment_takes_a_double_quoted_string_spark_shaped` + the `COMMENT "x.y" FIRST` row of `nested_parse_leaves_top_level_forms_to_the_existing_path` (red before), ANSI `every_recorded_nested_schema_cell_answers_spark_on_the_ansi_door` (green before: behaviour pin). |
| C-022 | (V-002, round 3) The CREATE nested-type token rewrite covers only the column-definition list on both doors; a CTAS query is passed through verbatim, and `CREATE TABLE … AS SELECT * FROM src WHERE map < 5 AND map > 0` answers Spark's row `[{map: 1}]`. | Token-level seam pins; answer + rows on the facade SQL and DataFrame doors, the ANSI door and the Rust Spark door. | **PROVEN** | seam `a_ctas_query_comparing_a_type_keyword_column_is_left_alone`, `only_the_column_definition_list_is_rewritten` (mutation: whole-statement rewrite reds both); `test_nested_ddl_{schema,rows}_matches_spark[ctas_where_map_lt-v{2,3}]`; ANSI `every_recorded_nested_ddl_cell_answers_spark_on_the_ansi_door` + `ctas_filtering_a_type_keyword_column_answers_spark_rows_on_the_ansi_door` (red before: `map ( 5 AND map ) 0`); Rust `ctas_filtering_a_map_column_with_lt_answers_spark_rows`. |
| C-023 | (V-002, round 3) `CREATE TABLE … AS SELECT * FROM src WHERE struct < 5 AND struct IS NOT NULL` answers Spark's row `[{struct: 1}]`. | Same pins as C-022. | **OPEN** (BACKLOG row IDENT-STRUCT-KW-1) | Not this unit's defect: sqlparser (`SparkSqlDialect` / `GenericDialect`, `supports_struct_literal`) reads an unquoted `struct` column in any expression as a STRUCT literal, so a plain `SELECT struct FROM t` / `WHERE struct = 1` refuses `ParseException` on the unchanged native too (measured). Strict-xfail `test_nested_ddl_{schema,rows}_matches_spark[ctas_where_struct_lt-v{2,3}]`, `#[ignore]` `ident_struct_kw_ctas_filtering_a_struct_column_with_lt_answers_spark_rows`, and the ANSI twins require the STRUCT-literal refusal and report the day it answers. Ruling Q-22b-NEST-9. |

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
entries (ruling Q-21a-NEST-1). `Cargo.lock` changes with the override and is never staged. Release
native built against fork `d9f226414` plus its worker's uncommitted
`nested_projection.rs` edit (the fork head moved to `c1bc78864` during the build).

Result: all eight adoption cells green on the SQL door and the DataFrame door with **no RePark
change** — the fork's field-id child matching and NULL fill is the whole reader fix.

Two RePark-side readings, both registered BACKLOG rows outside this unit's fence (ruling
Q-21a-NEST-2), measured first on the adopted `st_add_v2`:

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
(ruling Q-21a-NEST-3). Pinned strict-xfail `test_required_nested_child_message_matches_spark`.

## Step 5 — registry (2026-09-17)

`docs/spark-sql-iceberg-parity.md` §7, the unit's own block after ICE-COLUMN-REORDER-1-R-001:
ICE-NESTED-EVO-1 (FIXED, fork F-NESTED-EVO-1), ICE-NESTED-DDL-1 (FIXED), ICE-NESTED-DDL-1-R-001
(OPEN, required-child message), ICE-NESTED-INSERT-LIST-1 (OPEN, list `INSERT` in the fork
writer). `docs/map.md` carries the index line.

## Step 6 — gates, and one pin flipped on purpose (2026-09-17)

The whole facade suite (release native with the override) reddened
`test_catalog_surface_1.py::test_create_table_map_struct_refuse_loudly`: it pinned
`catalog.createTable` refusing map and struct schemas, which this unit makes work as Spark
does. Renamed `test_create_table_map_struct_round_trip`; it now asserts `listColumns` answers
`map<string,int>` and `struct<a:int>` (Spark's `simpleString`). The frozen
catalog-surface-1 ledger keeps the old name as history.

Gate counts (2026-09-18; release native with the fork override; fork clone at `c1bc78864` for
the first suite run, `41c2d4164` for the final build):

| gate | result |
|---|---|
| comment ban on `origin/main..HEAD` | `hits=0` |
| fork-clone path in the branch diff (`grep -c` over `git diff origin/main..HEAD`) | `0` |
| `cargo clippy --all-targets --all-features -- -D warnings` (brief's form) | exit 101: 36 errors, all `disallowed_methods` `unwrap`/`expect` in `crates/repark-ml` test modules (8 expect, 28 unwrap; untouched by this unit). Zero in any touched crate. The repo's own gates — `make rust-clippy` (`-A clippy::disallowed_methods`) and `make rust-panic-ban` (lib/bins) — are clean inside `make verify`. |
| `cargo test -p repark-spark --lib` | 1132 passed, 0 failed, 5 ignored (one is `forkwrite_list_insert_reads_back`) |
| `cargo test -p repark-iceberg --lib` | 450 passed, 0 failed |
| release native `maturin develop --release` | exit 0 |
| facade suite `-n 8`, run 1 (23:13 EDT) | 3 failed, 9730 passed, 398 skipped, 51 xfailed (1896 s). Failures: `test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren[ansi-off/ansi-on]` (known 20:00–24:00 EDT local red) and `test_catalog_surface_1.py::test_create_table_map_struct_refuse_loudly` (flipped on purpose, above; the file then 52 passed) |
| facade suite `-n 8`, run 2 (01:35 EDT, final native) | reached 99 % with zero failures, then hit the 3500 s wall-clock cap under load average 420–520 (three lanes on the box); not re-run |
| `test_ice_nested_evo_1.py` + `test_catalog_surface_1.py`, final native | 98 passed, 4 xfailed |
| `make verify` | exit 0 — `ci` gates clean; 58 Rust test binaries, 3818 passed, 0 failed, 8 ignored |

## Round 2 (2026-09-18) — run 22b

**Date:** 2026-09-18 · **Base:** `2f18da51` (round 1 + main with #676 / RP-25 fork #292 / RP-26
fork `8477b249`) · **Model:** Claude Opus 5 (high) · **Scope:** the Grok logic review's five P2s
(L-001 … L-006), the ledger gap audit (item 6), housekeeping. Round 2's first attempt was lost to
the 2026-09-18 OOM freeze; nothing of it survives. Rulings Q-21a-1 … 5 renamed Q-21a-NEST-1 … 5
(text unchanged).

### Step 1 — baseline (release native from `2f18da51`, no override)

`test_ice_nested_evo_1.py`: offline **46 passed, 4 xfailed**; live (`REPARK_PARITY_LIVE=1`)
**46 passed, 4 xfailed**. Every `fork292` pin is green on main's RP-25/RP-26 with no override.
The 4 xfails: 2 × `forkwrite` (list INSERT, fork F-1), the required-child whole line, EX-COL-2.

### Step 2 — Spark cells (one JVM per run, `jvm-lock.sh`)

`_record_ice_nested_evo_1.py` gained `build_schema_cells()` / `record_schema_cell()` (the table's
current metadata schema via `version-hint.text`, Spark's `SELECT *` schema, the refusal's first
non-empty line) and `record_dataframe_create()`. Four recorder runs (the first two added the
cells and the first-non-empty-line fix; the third the DataFrame-door cells; the fourth the two
double-quoted cells). Every run rewrote the fixture; the round-1 `cells` are byte-identical
(checked); the four adopted tables were re-copied (new file names) and the adoption pins stay
green on them. v2 = v3 on every new cell.

### The five P2s — measurement, decision, pins

| P2 | Spark 4.1.2 + Iceberg 1.11.0 (measured) | RePark before | Decision | Pins |
|---|---|---|---|---|
| L-001 CREATE ids | level order: `id`1 `s`2 `arr`3 `m`4, `a`5 `b`6 `c`7, `element`8 `x`9, `key`10 `value`11 `q`12 | **same** on the facade (probe) — the fork's `TableMetadataBuilder::new` → `reassign_ids` (Java `AssignFreshIds`) overwrites RePark's depth-first placeholders | PROVEN, no code (Q-22b-NEST-1) | C-014 |
| L-002 / L-006 dotted new name | backtick `x.y` accepted (leaf `x.y`; top-level `p.q` too); `TO "x.y"` / `s."x.y"` → `ParseException` `PARSE_SYNTAX_ERROR` near `'"x.y"'` 42601 | backtick: same; double-quoted: **accepted**, schema changed (silent wrong) | IMPLEMENTED (Spark door refusal, Rust) | C-015, R-002 |
| L-003 nested NOT NULL | `s.a` `"required": true`; DataFrame-door create: all optional | `ParseException … Expected: >, found: NOT` | IMPLEMENTED (both doors, Rust token rewrite + struct-field option) | C-016, R-003 |
| L-004 ANSI door | the recorded cells | 33 of the ANSI twins mismatched (`MAP<` parse error; v3 CREATE opt-in never read on a Spark-extended session; duplicate / unknown / required shapes) | IMPLEMENTED (Rust) | C-017, R-004 |
| L-005 FIRST / AFTER / COMMENT in a struct | `z` first (id 5), `w` after `a`, `d` with `doc` `c`; `AFTER s.a` → `PARSE_SYNTAX_ERROR` 42601 | **same** on both doors | PROVEN, no code | C-018 |

Item 6 — gaps closed: the unknown parent and duplicate child were claimed "refuses naming it"
with no Spark cell; measured `UNRESOLVED_COLUMN.WITH_SUGGESTION` 42703 / `FIELD_ALREADY_EXISTS`
42710 (`AnalysisException`), RePark raised `PySparkException` `DataInvalid => …` —
IMPLEMENTED (C-019, R-005). C-012 (required-child message) — IMPLEMENTED, now PROVEN
(Q-22b-NEST-4). C-020 (map / struct / array child types) — PROVEN, no code. C-008 stays
**OPEN**: rows after an `INSERT` into a list column need the fork writer (F-1,
ICE-NESTED-INSERT-LIST-1, strict xfail `forkwrite-…list_element_child_add_read`, fork ask
F-1 in the round-1 hand-back); its DDL half is PROVEN (C-017 twins, `_describe` cells).

### Step 3 — RED (pins written before the fix)

- Facade, `test_ice_nested_evo_1_schema.py` on the baseline native: **12 failed, 76 passed**.
  10 real: `create_required_child` × 6 (metadata, outcome, read; v2, v3), `add_duplicate_child`
  × 2, `add_unknown_parent` × 2. 2 were my wrong expectation — the DataFrame-door create
  pinned against the SQL door's metadata; the Spark DataFrame-door recording (`asNullable`)
  shows RePark was right, and the pin now reads `dataframe_create_cells` (Q-22b-NEST-2). The
  double-quoted cells, recorded next, were red on the same native (accepted; schema changed:
  `s.a` renamed to `x.y`, child `x.y` added).
- ANSI door, `ansi_nested_ddl_oracle.rs` against pre-round sources (`git stash` of `src/`):
  **0 passed, 2 failed**, 33 mismatching cells — v2: `map_value_child_add_read` / `_describe`
  (`ParserError("Expected: (, found: <")`), `create_field_ids`, `create_required_child`,
  `add_map_child`, `add_duplicate_child`, `add_unknown_parent`; v3: every cell (`WITH
  'format_version' = '3' is not enabled`).
- The round-1 message pin `test_required_nested_child_message_matches_spark` (strict xfail
  until now) was red as a plain pin at commit 1.

### Step 4 — implementation (Rust; Python forwards nothing new)

- `repark-iceberg/src/write/nested_type_sql.rs` (new): `rewrite_nested_type_tokens`,
  `has_nested_type_opener`, `struct_field_required`.
- `repark-iceberg/src/write/nested_column.rs`: `nested_add_refusal`,
  `nested_required_add_refusal`, Spark `DataType.sql` rendering; `column_move.rs` exposes
  `unresolved_column` / `top_level_names` to its sibling.
- Spark door: `normalize.rs` (rewrite on CREATE), `create_table.rs` (required child),
  `nested_column_ddl.rs` (double-quoted refusal, the two pre-checks).
- ANSI door: `router.rs` + `create_table/nested_type.rs` (CREATE rewrite, structural type
  mapping), `create_table.rs` (dispatch, Arrow via `type_to_arrow_type`, the typed v3 opt-in
  read), `alter/nested.rs` (rewritten tokens, the two pre-checks).

### Rulings

- **Q-22b-NEST-1 — L-001 needs no code.** The fork's fresh-id assignment already produces
  Spark's ids; RePark's `alloc_field_id` numbering is a placeholder the fork overwrites. It is
  kept (it also numbers ALTER child types, which `UpdateSchema` reassigns the same way) and
  the result is pinned on three doors.
- **Q-22b-NEST-2 — the DataFrame-door CREATE answers Spark's DataFrame door.** Spark's V2 CTAS
  applies `asNullable`, so `writeTo(t).create()` with a `NOT NULL` child writes it optional;
  recorded as `dataframe_create_cells` and pinned against that, not the SQL door's metadata.
- **Q-22b-NEST-3 — `"x.y"` on the ANSI door is a delimited identifier.** Standard SQL (and
  `GenericDialect`) read double quotes as an identifier; Spark reads a string literal. The
  ANSI twin of the two double-quoted cells is Spark's backtick cell; the ANSI door accepting
  `"x.y"` as the leaf `x.y` is the intended door difference.
- **Q-22b-NEST-4 — the required-child line is raised in RePark.** Spark's line is
  `SparkCatalog`'s `Unsupported table change: ` prefix over Iceberg 1.11.0's `Incompatible
  change: cannot add required column: <leaf>`; RePark raises it before the fork is asked
  (the fork is never patched, rule 3), as a base-class error (`PySparkException`) because
  Spark's is a `SparkException`, not an `AnalysisException`. Supersedes the prefix question of
  Q-21a-NEST-3. The fork's own text (F-2) no longer surfaces through nested DDL.
- **Q-22b-NEST-5 — the ANSI v3 opt-in fix is in scope.** It blocked every v3 ANSI twin; the
  lookup scanned `entries()` for `repark.sql.allow_create_format_version_3`, which
  DataFusion 54 lists as `allow_create_format_version_3`. The typed read (the ALTER path's)
  is added; the key scan stays for the in-crate stand-in config the V3-2 tests use.
- **Q-22b-NEST-6 — the ANSI twins replay the DDL half.** Spark's `INSERT … SELECT
  named_struct(…) / array(…) / map(…)` spellings are Spark SQL; rows are pinned on the
  facade's SQL and DataFrame doors, the ANSI twins pin every read's names and types, every
  `DESCRIBE`, every metadata schema and every refusal.
- **Q-22b-NEST-7 — `STATUS.md` is not edited this round.** The brief allows deleting the
  V2-10d clause only when every clause is PROVEN or DECLARED; C-008 is OPEN (fork F-1).

### Step 5 — gates (RULE 2 only)

| gate | result |
|---|---|
| release native `maturin develop --release` (codegen-units 16, 6 jobs) | exit 0 (three builds: baseline, after the fixes, after C-012) |
| `test_ice_nested_evo_1.py` offline / live | **48 passed, 3 xfailed** / **48 passed, 3 xfailed** (xfails: 2 × `forkwrite`, EX-COL-2) |
| `test_ice_nested_evo_1_schema.py` offline / live | **100 passed** / **100 passed** |
| `cargo test -p repark-iceberg` | 2 binaries, 515 passed, 0 failed |
| `cargo test -p repark-sql` | 22 binaries, 455 passed, 0 failed |
| `cargo test -p repark-spark` | 10 binaries, 1228 passed, 0 failed, 5 ignored |
| `cargo clippy -p repark-iceberg -p repark-sql -p repark-spark --tests -- -D warnings` | exit 101: every error is `clippy::disallowed_methods` (`unwrap` / `expect`) in test code across the three crates, pre-existing and in the new test code alike (5710 hits; round 1 measured the same class); `make rust-clippy`'s own form (`-A clippy::disallowed_methods`) on the same crates: **exit 0**; lib/bin `-- -D warnings` with the lint active (the panic-ban surface): **exit 0** |
| `cargo fmt --check`, pre-commit hook (map lockstep, map-sync, crate DAG, file-size, docstrings, manifest, taplo, typos) | clean on every commit |
| `python3 scripts/check_docs_links.py` | clean (975 files, 5901 links) |
| comment ban `comment_ban.py /tmp/lb-build origin/main` | `hits=0` |
| `[patch]` / path override / session path in `git diff 2f18da51..HEAD` | 0 |

### Open

- C-008 — rows after an `INSERT` into a list column (fork F-1, ICE-NESTED-INSERT-LIST-1).
  COVERAGE_ATTESTATION withheld: not every clause is PROVEN or DECLARED.
- Not measured this round, no clause: a `COMMENT` inside a CREATE struct type
  (`STRUCT<a: INT COMMENT 'x'>`) still refuses at the parser on both doors (a loud
  `ParseException`); the `UNRESOLVED_COLUMN` suggestion list is the top-level columns in
  schema order where Spark ranks by similarity (they agree on the recorded cell); EX-COL-2 and
  COL-DOTTED-FIELD-1 stay BACKLOG.

**Orchestrator ruling Q-22b-4 (run 22b, 2026-09-18).** The STATUS.md residue clause for V2-10d reads
"a Spark-added nested child is unreadable". That is no longer true on main: the adoption reads are
green on RP-25 / RP-26 with no override (C-001..C-005, the `fork292` pins, offline and live). The
V2-10d clause is deleted in this PR. C-008 stays OPEN for a different defect: every `INSERT` into an
Iceberg list column fails in the fork writer (ICE-NESTED-INSERT-LIST-1, fork F-LIST-INSERT-1, run 22a's
fork lane). Its strict xfails flip when that fork fix reaches RePark in RP-27.

## Round 3 (2026-09-18) — run 22b verification findings

Grok 4.6's verification of `e78a94f7` found V-001 (P1) and V-002 (P2). The orchestrator
confirmed both on Spark 4.1.2 + Iceberg 1.11.0 (`probe_comment.py`). Branch
`fix/ice-nested-evo-1`, from `e78a94f7`.

### Cells (recorded by the driver, one JVM, `jvm-lock.sh`; v2 and v3 identical)

| label | statement (on `(id INT, s STRUCT<a: INT, b: STRING>)` / a one-row source) | Spark 4.1.2 |
|---|---|---|
| `add_comment_double_quoted_dotted` | `ADD COLUMN s.d INT COMMENT "x.y"` | ok; `s` = `a, b, d(doc x.y)` |
| `add_comment_double_quoted` | `ADD COLUMN s.e INT COMMENT "c"` | ok; `s` = `a, b, e(doc c)` |
| `add_comment_double_quoted_first` | `ADD COLUMN s.f INT COMMENT "x.y" FIRST` | ok; `s` = `f(doc x.y), a, b` |
| `ctas_where_map_lt` | `CREATE TABLE … USING iceberg TBLPROPERTIES (…) AS SELECT * FROM src WHERE map < 5 AND map > 0` | ok; rows `[{map: 1}]` |
| `ctas_where_struct_lt` | `… AS SELECT * FROM src WHERE struct < 5 AND struct IS NOT NULL` | ok; rows `[{struct: 1}]` |

These are the probe's answers. The driver re-recorded every cell. Every earlier `cells`,
`schema_cells` and `dataframe_create_cells` entry answers byte-identically (compared key by
key). The four adoption tables it re-copied were restored from git, not re-committed. The
fixture has no machine-local path beyond the existing baked root.

### Step 1 — baseline

A release native built from `e78a94f7` sources with the round-3 edits stashed (the first build
was stopped: it could have caught a half-written edit): `test_ice_nested_evo_1.py` +
`test_ice_nested_evo_1_schema.py` `-n 4`, offline — the 148 pre-existing tests passed and 3
xfailed, as round 2 left them.

### Step 2 — RED (pins written first, fix stashed, same baseline native / sources)

- Facade, `-n 4` offline: **22 failed**, 152 passed, 3 xfailed. Failures:
  `test_nested_ddl_{metadata,outcome,read_schema}_matches_spark[add_comment_double_quoted{,_dotted,_first}-v{2,3}]`
  (18, refused `PARSE_SYNTAX_ERROR`) and `test_nested_ddl_{schema,rows}_matches_spark[ctas_where_struct_lt-v{2,3}]`
  (4). The `ctas_where_map_lt` facade cells passed on the Spark door before the fix: there the
  rewrite (no `map_parens`) re-emits `map < … >` unchanged. The door that corrupted `map` is the ANSI door.
- Rust Spark door (`cargo test -p repark-spark --lib`): `nested_parse_leaves_top_level_forms_to_the_existing_path`,
  `nested_add_comment_takes_a_double_quoted_string_spark_shaped`, and the CTAS pin: 3 failed.
- ANSI door (`--test ansi_nested_ddl_oracle`): `every_recorded_nested_ddl_cell_answers_spark_on_the_ansi_door`
  and `ctas_filtering_a_type_keyword_column_answers_spark_rows_on_the_ansi_door` failed. `ctas_where_map_lt`
  refused `Expected: end of statement, found: 0` (`map ( 5 AND map ) 0`). `ctas_where_struct_lt` refused
  `Expected: a data type name, found: 5`. The schema-cell twin stayed green: the ANSI door has no
  double-quote refusal, so its `COMMENT "…"` cells pin behaviour, not the V-001 fix. That was
  recorded in the brief's P3.

### Step 3 — the fixes (Rust)

- **V-001**, `crates/repark-spark/src/nested_column_ddl.rs`: the whole-statement
  `first_double_quoted_word` scan is gone. A `NameParser { parser, double_quoted }` wraps the
  sqlparser `Parser`. `name()` / `column_path()` record the first identifier with quote style
  `"` read in a name position: a path segment, the name after `TO`, the `AFTER` reference.
  A claimed statement refuses with the same verbatim `PARSE_SYNTAX_ERROR` near `'"x.y"'`
  (C-015 unchanged). `COMMENT` still reads through `parse_literal_string`, where a double-quoted
  token is a string.
- **V-002**, `crates/repark-iceberg/src/write/nested_type_sql.rs`: `column_list_range` finds
  the first parenthesized group after `TABLE [IF NOT EXISTS] <name>`.
  `rewrite_create_column_types` runs the unchanged `rewrite_nested_type_tokens` on that range
  only, and `create_column_list_has_nested_type_opener` is the gate over the same range. Callers:
  `crates/repark-spark/src/normalize.rs` (`parse_single_normalized`, every `CREATE TABLE`) and
  `crates/repark-sql/src/create_table/nested_type.rs` (`rewrite_nested_create_types`, the ANSI
  router). A CTAS with no column list is passed through untouched. The ANSI `ALTER` nested
  pre-parse keeps `rewrite_nested_type_tokens`: an `ALTER … ADD COLUMN` has no query.
- After the V-002 fix, `ctas_where_struct_lt` still refuses on both doors. The error is sqlparser's
  own STRUCT-literal parse (Spark door `Expected: end of statement, found: USING` after
  `parse_single_normalized` falls back; ANSI `Expected: a data type name, found: 5`). A
  facade probe on the unchanged native: `SELECT struct FROM t`, `… WHERE struct < 5`,
  `… WHERE struct IS NOT NULL`, `… WHERE struct = 1` all refuse `ParseException`;
  `` SELECT `struct` … WHERE `struct` < 5 `` answers. Ruling Q-22b-NEST-9.

### Pins (green after the fix)

- C-021 — facade `test_nested_ddl_{metadata,outcome,read_schema}_matches_spark[add_comment_double_quoted*-v{2,3}]`;
  Rust `nested_add_comment_takes_a_double_quoted_string_spark_shaped`, `nested_parse_leaves_top_level_forms_to_the_existing_path`;
  ANSI `every_recorded_nested_schema_cell_answers_spark_on_the_ansi_door`. The round-2 double-quoted
  refusals (`TO "x.y"`, `s."x.y"`) stay green: `nested_ddl_refuses_double_quoted_names_and_known_paths_spark_shaped`,
  `[rename_double_quoted,add_double_quoted_leaf-v{2,3}]`.
- C-022 — seam `a_ctas_query_comparing_a_type_keyword_column_is_left_alone`,
  `only_the_column_definition_list_is_rewritten` (mutation run: replacing the range with the
  whole statement reds both); facade `test_nested_ddl_{schema,rows}_matches_spark[ctas_where_map_lt-v{2,3}]`
  (the `SELECT *` read now also runs on the DataFrame door); ANSI
  `ctas_filtering_a_type_keyword_column_answers_spark_rows_on_the_ansi_door` (Spark's rows,
  inserts replayed) and `every_recorded_nested_ddl_cell_answers_spark_on_the_ansi_door`;
  Rust `ctas_filtering_a_map_column_with_lt_answers_spark_rows`.
- C-023 — strict-xfail `[ctas_where_struct_lt-v{2,3}]` (schema + rows), `#[ignore]`
  `ident_struct_kw_ctas_filtering_a_struct_column_with_lt_answers_spark_rows`, and ANSI twins
  that require the STRUCT-literal refusal and fail the day it answers.

### Rulings

- **Q-22b-NEST-8 — the double-quoted refusal is positional.** Spark reads a double-quoted
  token as a string literal. It refuses only where the grammar wants an identifier. The Spark-door
  nested pre-parse records a double-quoted identifier only from its name positions (path
  segments, `TO`, `AFTER`), and `COMMENT "…"` is the child's `doc`. A `DEFAULT` value is not
  parsed by the nested pre-parse (it refuses as before, not measured). A single-quoted identifier
  (`s.'x'`) is left as round 2 had it (not measured).
- **Q-22b-NEST-9 — `struct` as a column name in an expression is its own row.** The V-002
  corruption (`NOT NULL` → `OPTIONS(…)`, `MAP<` → `MAP(`) is fixed and pinned at token level. The
  measured `struct` CTAS still refuses, because sqlparser reads an unquoted `struct` in any
  expression as a STRUCT literal (`supports_struct_literal` is true for `SparkSqlDialect`,
  `DatabricksDialect` and `GenericDialect`). That refusal predates this unit, holds for a plain
  `SELECT`, and is loud (`ParseException`). A fix is a general expression-parse change for both
  doors, which this nested-DDL unit does not take. It is proposed as registry row
  **IDENT-STRUCT-KW-1** (BACKLOG, orchestrator to file). Its cells are strict-xfail /
  expected-refusal so the fix flips them.
- **Q-22b-NEST-10 — the fixture is extended, not rebuilt.** The driver re-records everything.
  The committed `oracle.json` carries the new cells and byte-identical old ones. The four
  adoption tables stay at their round-1 bytes, because their `v4.metadata.json` is what the
  adoption pins register.

### Step 4 — gates (RULE 2 only; after the fix, on the final tree)

| gate | result |
|---|---|
| release native `maturin develop --release` (codegen-units 16, 6 jobs) | exit 0 (pre-fix baseline build, and the fixed build) |
| `test_ice_nested_evo_1.py` offline / live | **52 passed, 7 xfailed** / **52 passed, 7 xfailed** (xfails: 2 × `forkwrite`, EX-COL-2, 4 × IDENT-STRUCT-KW-1) |
| `test_ice_nested_evo_1_schema.py` offline / live | **118 passed** / **118 passed** |
| both files together, `-n 4` offline | 170 passed, 7 xfailed |
| `cargo test -p repark-iceberg` | 2 binaries, 527 passed, 0 failed |
| `cargo test -p repark-sql` | 22 binaries, 456 passed, 0 failed |
| `cargo test -p repark-spark` | 10 binaries, 1249 passed, 0 failed, 6 ignored |
| `cargo clippy -p repark-iceberg -p repark-sql -p repark-spark --tests -- -D warnings -A clippy::disallowed_methods` | exit 0 |
| `cargo clippy -p repark-iceberg -p repark-sql -p repark-spark -- -D warnings` | exit 0 |
| `cargo fmt --check`, pre-commit hook (map lockstep, map-sync, crate DAG, file-size, docstrings, manifest, taplo, typos) | clean on both fix commits |
| comment ban `comment_ban.py /tmp/lb-build origin/main` | `hits=0` |
| `[patch]` / path override / session path in `git diff e78a94f7..HEAD` | 0 |

The V-001 commit's intermediate tree (without the CTAS cells) was checked on its own:
`cargo test -p repark-spark --lib nested_column_ddl` 10 passed / 1 ignored, and
`--test ansi_nested_ddl_oracle` 2 passed.

### Open after round 3

- C-008 — unchanged (fork F-1, ICE-NESTED-INSERT-LIST-1).
- C-023 — DECLARED on IDENT-STRUCT-KW-1 (Q-22b-NEST-9).
- Residual, record only: a `COMMENT` on a struct field inside CREATE (`STRUCT<a: INT COMMENT
  'x'>`) is still a loud parser refusal on both doors.

## WO U5 PR1 plan (2026-09-24)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-024 | Nested `ALTER COLUMN <path> TYPE <primitive>` promotes a struct field, list element, or map value in Iceberg metadata. | Spark cell replay plus Rust and facade metadata pins. | PROVEN | Spark 4.1.2 recorded `struct<a:long>`, `list<long>`, and `map<string,long>`; the RePark replay reports the same metadata schemas. |
| C-025 | `ALTER TABLE … UNSET TBLPROPERTIES IF EXISTS ('missing')` succeeds without changing metadata. | Spark cell replay plus a Rust and facade no-op pin. | PROVEN | Spark accepts both missing-key spellings with unchanged properties. RePark preserves the empty property map for `IF EXISTS` and the existing bare spelling. |
| C-026 | `ALTER NAMESPACE … SET DBPROPERTIES ('b'='2')` updates the catalog namespace and `DESCRIBE NAMESPACE EXTENDED` renders `((b,2))`. | Spark cell replay plus Rust and facade rows/schema pins. | PROVEN | Spark accepts SET DBPROPERTIES and SET PROPERTIES, sorts `a`, `x`, `z`, and renders `((b,2))`; RePark merges through `Catalog::update_namespace` and replays `((b,2))`. |
| C-027 | A nested `ALTER COLUMN <path> TYPE <primitive>` (map key, struct field or list element) answers each from→to pair the way Spark 4.1.2 does. A pair Spark's `Cast.canUpCast` rejects raises `NOT_SUPPORTED_CHANGE_COLUMN` (AnalysisException, SQLSTATE `0A000`) with Spark SQL type names. An up-castable pair that is not an Iceberg promotion raises `Unsupported table change: Cannot change column type: <path>: <from> -> <to>`. An Iceberg promotion on a map key raises `Unsupported table change: Cannot update map keys: map<…>`. The same promotion on a struct field or list element succeeds, and a same-type map key is a no-op. | Spark probes plus a Rust table test and facade pins for each branch. | PROVEN | Spark 4.1.2 + Iceberg 1.11.0 measured on 2026-09-24 (`target/probe-u5-r1fix/spark*.summary`). Map key: INT→BIGINT, FLOAT→DOUBLE and DECIMAL(9,2)→DECIMAL(12,2) give `Cannot update map keys`. INT→STRING, INT→DOUBLE and BIGINT→STRING give `Cannot change column type`. STRING→BIGINT and BIGINT→INT give `NOT_SUPPORTED_CHANGE_COLUMN` `"STRING"`/`"BIGINT"`/`"INT"`. The same pairs on `st.*` and `arr*.element` answer the same way, and INT→BIGINT succeeds there. Further measured pairs: INT→DECIMAL(10,0), DATE→TIMESTAMP, TIMESTAMP→BIGINT, INT→FLOAT and BOOLEAN→STRING (Iceberg refusal), INT→DECIMAL(9,0) and DECIMAL(9,2)→DOUBLE (analysis refusal), and STRUCT→STRING (analysis refusal, `"STRUCT<x: …>"`). The rule lives in `repark_iceberg::write::nested_column::resolve_nested_type_change`. Pins: `nested_alter_column_type_refuses_each_pair_like_spark` / `…_accepts_the_pairs_spark_accepts`, `test_nested_alter_column_type_refusals_match_spark`. A refusal is a PySparkException without a condition, where Spark's is a Py4J-wrapped SparkException; pyspark has no SparkException class. |
| C-028 | The three parser additions remain narrow: top-level TYPE, non-primitive top-level TYPE, and UNSET without IF EXISTS preserve their prior routes. | Direct Rust parser pins and facade near-miss pins. | PROVEN | Parser pins leave top-level TYPE and nested column-move forms on their prior paths. Mutation runs red for the nested commit, UNSET rewrite, and namespace property merge. |

## WO U5 PR1 round 2 (critic r1 V-001..V-005, 2026-09-24)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-029 | A nested `ALTER COLUMN … TYPE` path resolves the way Spark's does. Struct steps match case-insensitively. A map `key`/`value` or list `element` step must be spelled in lower case, and a step under a map, a list or a primitive raises `[INVALID_FIELD_NAME] Field name <typed> is invalid: <resolved> is not a struct. SQLSTATE: 42000`. An unknown step raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` with the top-level names in schema order. A missing table raises `TABLE_OR_VIEW_NOT_FOUND` instead of the raw catalog `TableNotFound`. | Spark probe plus Rust and facade pins. | PROVEN | Measured: `m.KEY`/`M.KEY`/`m.Value`/`arr.ELEMENT`/`id.x`/`st.a.q` give INVALID_FIELD_NAME. `st.zz`/`nope.x`/`st.zz.q` give UNRESOLVED_COLUMN.WITH_SUGGESTION with the schema-order list. `ST.INNER.X` succeeds. A missing table gives TABLE_OR_VIEW_NOT_FOUND 42P01. RePark's TABLE_OR_VIEW_NOT_FOUND text comes from the shared builder, which joins Spark's three sentences with spaces where Spark uses newlines (pre-existing). |
| C-030 | `ALTER NAMESPACE … SET [DB]PROPERTIES` follows Spark's parser. A missing namespace raises `[SCHEMA_NOT_FOUND] The schema `<ns>` cannot be found. … SQLSTATE: 42704`. `location` and `owner` (exact lower case) raise `UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY` (0A000), and the first reserved key in statement order wins. A duplicate key raises `DUPLICATE_KEY` (23505) before the reserved-key check. `()`, a bare-word value, a numeric key, a signed value, a missing value after `=`, an unclosed list and a trailing token raise `PARSE_SYNTAX_ERROR`. `comment`, `LOCATION`, dotted and word keys, numeric and boolean values (booleans lower-cased), double-quoted strings and a missing `=` are accepted. | Spark probe plus Rust and facade pins. | PROVEN | Measured on Spark 4.1.2 (`spark.summary`, `spark2.summary`). The reserved-key and parse refusals fire before the namespace lookup, as in Spark (`ALTER NAMESPACE sc.nope SET DBPROPERTIES ('location'=…)` gives the reserved refusal). DESCRIBE NAMESPACE EXTENDED after `'comment'='c'` shows the `Comment` row and leaves `comment` out of `Properties`, as Spark does. Residue: `('nv')` with no value raises a ParseException with Spark's `Operation not allowed: Values must be specified for key(s): [nv].` text wrapped as `SQL error: ParserError(…)`, and without Spark's `_LEGACY_ERROR_TEMP_0035` condition. |
| C-031 | `UNSET TBLPROPERTIES` drops only an `IF` that `EXISTS` immediately follows (and that `EXISTS`). A lone `IF` raises `Syntax error at or near '<next>': missing 'EXISTS'`, `IF` at the end of input raises `… near end of input`, and a leading `EXISTS` raises `… near '<EXISTS as typed>': extra input '<EXISTS as typed>'`, each `PARSE_SYNTAX_ERROR` 42601 with properties unchanged. | Spark probe plus Rust and facade pins. | PROVEN | Measured: `IF ('nope')`/`if ('nope')` give missing 'EXISTS', `EXISTS (…)`/`exists (…)`/`EXISTS IF (…)` give extra input, `IF` gives end of input, and `if exists (…)` is accepted. Pins: `unset_tblproperties_takes_only_the_if_exists_pair`, `test_unset_tblproperties_if_without_exists_matches_spark`. |

### Out-of-scope observation

- Spark accepts `ALTER NAMESPACE ... UNSET DBPROPERTIES (...)` and `UNSET PROPERTIES (...)`. This PR does not add the distinct namespace UNSET operation.
- Round 2 residues, measured: (1) `SET DBPROPERTIES ('k' 'v')` is accepted by Spark as `(k,v)`, but RePark's shared `spark_literals::canonicalize` joins the adjacent literals into one key `kv` before the intercept runs, so RePark refuses it with the missing-value text. (2) `SET DBPROPERTIES ('location_uri'='/y')` is accepted by both engines, but RePark's catalog layer reads `location_uri` as the namespace location, so DESCRIBE shows a `Location` row and new tables land under `/y`. Spark keeps it as a plain property. (3) `ALTER VIEW v UNSET TBLPROPERTIES IF ('nope')` goes through the view parser and answers `could not parse CREATE NAMESPACE: … Expected: (, found: IF`, where Spark gives `PARSE_SYNTAX_ERROR` missing 'EXISTS'. (4) The top-level `ALTER COLUMN id TYPE …` route still surfaces the fork's `DataInvalid` for Iceberg refusals. The per-pair rule in C-027 covers nested paths only.

## WO U5 PR1 round 3 (critic r2 V-001..V-003, 2026-09-24)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-032 | A nested `ALTER COLUMN <path> TYPE` whose target is `TINYINT`, `SMALLINT`, `CHAR(n)`, `CHARACTER(n)` or `VARCHAR(n)` (any case) is decided from the SQL type before it is lowered to Iceberg. The path resolves first (C-029). Then the statement raises `NOT_SUPPORTED_CHANGE_COLUMN` (0A000) with Spark's target name `"TINYINT"`, `"SMALLINT"`, `"CHAR(n)"` or `"VARCHAR(n)"`, whatever the from type is, map keys included. | Spark probe plus the Rust refusal table and facade rows. | PROVEN | Measured on Spark 4.1.2 + Iceberg 1.11.0 (`target/probe-u5-r2fix/spark.summary`, `spark2.summary`, `spark3.summary`): INT→TINYINT/SMALLINT, BIGINT→SMALLINT/TINYINT, INT→VARCHAR(10)/CHAR(5)/CHARACTER(5), STRING→VARCHAR(10)/CHAR(5), STRUCT→VARCHAR(10), list element INT→TINYINT, map key INT→SMALLINT and map value BIGINT→SMALLINT all give NOT_SUPPORTED_CHANGE_COLUMN. The map key gives no `Cannot update map keys`. Lower-case `tinyint` and `varchar(10)` render upper case. `st.zz`/`st.a.q` still give the path refusal, and a missing table gives TABLE_OR_VIEW_NOT_FOUND. Before this round RePark committed a silent no-op for INT→TINYINT/SMALLINT and STRING→CHAR/VARCHAR, and named `"INT"` for BIGINT→SMALLINT. Pinned pairs (Rust and facade): struct field INT→TINYINT (also lower-case `tinyint`), INT→SMALLINT, BIGINT→SMALLINT, BIGINT→TINYINT, INT→VARCHAR(10), INT→CHAR(5), INT→CHARACTER(5), STRING→VARCHAR(10) (also lower-case), STRING→CHAR(5), STRUCT→VARCHAR(10); list element INT→TINYINT; map key INT→SMALLINT; map value INT→SMALLINT and, since round 4, the measured map value BIGINT→SMALLINT (`target/probe-u5-r3fix/spark.summary` `m_value_smallint`, pinned in `nested_alter_column_type_decides_bare_decimal_and_map_values_like_spark` and `test_nested_alter_column_type_bare_decimal_and_map_value_match_spark`). The map value INT→SMALLINT row is RePark-only; Spark's measured map value is BIGINT. Pins: `spark_only_target_refusal_cases` in `nested_alter_column_type_refuses_each_pair_like_spark`, and `test_nested_alter_column_type_refusals_match_spark`. Mutation: without the TINYINT arm, the Rust table goes red at `st.a TYPE TINYINT` (the statement succeeds). |
| C-033 | A nested `ALTER COLUMN <path> TYPE` target Spark's parser refuses is a ParseException before the table is loaded. Bare `CHAR`, `CHARACTER` and `VARCHAR` raise `[DATATYPE_MISSING_SIZE] DataType "<NAME>" requires a length parameter, for example "<NAME>"(10). Please specify the length. SQLSTATE: 42K01`. `TINYINT(n)`, `SMALLINT(n)`, `INT(n)`, `INTEGER(n)`, `BIGINT(n)`, `FLOAT(p)`, `DOUBLE(p)`, `TIMESTAMP(p)` and, since round 4, `STRING(n)`, `BINARY(n)`, `FLOAT(p,s)`, `DOUBLE(p,s)`, `TIMESTAMP_NTZ(p)`, `DATE(n)` and `BOOLEAN(n)` raise `[UNSUPPORTED_DATATYPE] Unsupported data type "<TYPE(n)>". SQLSTATE: 0A000`, with the typed keyword upper-cased and parameters joined by a bare comma. | Spark probe plus Rust and facade pins. | PROVEN | Measured (`spark2.summary`, `spark3.summary`; round 4 `target/probe-u5-r3fix/spark.summary`: `STRING(10)`, `string(10)`, `DOUBLE(5,2)`, `double(5,2)`, `FLOAT(10,2)`, `BINARY(3)`, `TIMESTAMP_NTZ(3)`, `DATE(3)`, `BOOLEAN(1)`), including `varchar`/`Character` spellings and a missing table (DATATYPE_MISSING_SIZE still wins). Before this round each was a silent success or a lowered-type answer. Pins: `nested_alter_column_type_refuses_unsized_and_sized_targets_as_parse_errors`, `test_nested_alter_column_type_target_parse_refusals_match_spark`. PySpark appends a `== SQL ==` context block that RePark does not render, the same as every other RePark ParseException. |
| C-034 | C-027's per-pair rule holds for the Spark type names pinned in C-027 and C-032. It does not cover non-Spark aliases that the shared CREATE TABLE lowering accepts, or non-primitive targets. Those are listed below. | Ledger narrowing plus facade rows for each Rust-pinned C-027 pair. | PROVEN | Facade rows now mirror every Rust-pinned C-027 pair (DECIMAL(9,2)→DOUBLE, INT→DECIMAL(9,0), STRUCT→STRING, INT→DOUBLE, INT→FLOAT, DATE→TIMESTAMP, TIMESTAMP→BIGINT, INT→DECIMAL(10,0), BOOLEAN→STRING, FLOAT map key→DOUBLE). |

### Round 3 out-of-scope observation

- Non-primitive nested targets (V-002), measured and not matched: Spark refuses by target kind before any from→to check. `st.inner TYPE STRUCT<x: BIGINT>` and `st.a TYPE STRUCT<x: INT>` raise `[CANNOT_UPDATE_FIELD.STRUCT_TYPE] Cannot update `sc`.`u5`.`t` field `st`.`inner` type: Update a struct by updating its fields. SQLSTATE: 0A000`. `arr2.element TYPE ARRAY<BIGINT>` and `st.a TYPE ARRAY<INT>` raise `[CANNOT_UPDATE_FIELD.ARRAY_TYPE] Cannot update … field `arr2`.`element` type: Update the element by updating `arr2`.`element`.element. SQLSTATE: 0A000`. `st.a`/`m.value TYPE MAP<INT, INT>` raise `[CANNOT_UPDATE_FIELD.MAP_TYPE] Cannot update … field `st`.`a` type: Update a map by updating `st`.`a`.key or `st`.`a`.value. SQLSTATE: 0A000`. All are AnalysisException. RePark raises UnsupportedOperationException `This feature is not implemented: ALTER COLUMN `st.inner` TYPE to a non-primitive is not supported`, with no condition. This is a loud refusal of another class. Matching it needs three new conditions in `repark_common::spark_error`, which is more than one arm.
- `BYTE` and `SHORT` targets: Spark reads them as TINYINT/SMALLINT (NOT_SUPPORTED_CHANGE_COLUMN `"INT"` → `"TINYINT"`). RePark's shared lowering refuses them as `column type `BYTE` is not supported yet for Iceberg tables` (UnsupportedOperationException). Loud.
- Non-Spark aliases that the shared `sql_type_to_iceberg_with_timestamp_type` lowering accepts (`TEXT`, `INT2`/`INT4`/`INT8`, `FLOAT4`/`FLOAT8`, `BOOL`, `NVARCHAR`, `CHARACTER VARYING`, `DOUBLE PRECISION`, `TIMESTAMP(p) WITH TIME ZONE`) are unmeasured on the nested TYPE route. They lower to a Spark type and follow C-027. The same lowering serves CREATE TABLE and top-level ALTER, so this is pre-existing. `FLOAT(p,s)` is measured since round 4 and refused per C-033.
- Top-level `ALTER COLUMN id TYPE TINYINT` (id INT) still succeeds as a no-op. Spark raises NOT_SUPPORTED_CHANGE_COLUMN `"INT"` → `"TINYINT"` (measured). Pre-existing on the top-level route, which this PR leaves unchanged (C-028). Top-level `TYPE VARCHAR` (bare) is DATATYPE_MISSING_SIZE in Spark and unchanged here.

## WO U5 PR1 round 4 (critic r3 V-001..V-004, 2026-09-24)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-035 | A nested `ALTER COLUMN <path> TYPE DECIMAL`, `NUMERIC` or `DEC` with no precision targets `decimal(10,0)`, Spark's default, before the Iceberg lowering. A `DECIMAL(38,18)` field raises `NOT_SUPPORTED_CHANGE_COLUMN` from `"DECIMAL(38,18)"` to `"DECIMAL(10,0)"` (0A000). An INT field raises `Unsupported table change: Cannot change column type: st.i: int -> decimal(10, 0)`. | Spark probe plus Rust and facade pins. | PROVEN | Measured on Spark 4.1.2 + Iceberg 1.11.0 (`target/probe-u5-r3fix/spark.summary` `dec_bare_a`, `dec_bare_i`, `numeric_bare_i`, `dec_short_i`) over `sc.u5.d (st STRUCT<a: DECIMAL(38,18), i: INT>)`. Before this round RePark read the bare type as `decimal(38,18)`: a silent success on `st.a` and `int -> decimal(38, 18)` on `st.i`. Pins: `nested_alter_column_type_decides_bare_decimal_and_map_values_like_spark`, `test_nested_alter_column_type_bare_decimal_and_map_value_match_spark`. Nested route only; the global default is residue R-U5-DECIMAL-DEFAULT below. |
| C-036 | A nested `ALTER COLUMN … TYPE` on a table whose namespace does not exist raises `[TABLE_OR_VIEW_NOT_FOUND] The table or view `<catalog>`.`<ns>`.`<table>` cannot be found. … SQLSTATE: 42P01`, the same as a missing table. | Spark probe plus Rust and facade pins. | PROVEN | Measured: `ALTER TABLE sc.nope.t ALTER COLUMN st.a TYPE BIGINT` gives TABLE_OR_VIEW_NOT_FOUND `sc`.`nope`.`t` (`ns_missing`). Before this round RePark leaked `NamespaceNotFound => No such namespace: NamespaceIdent(["nope"])`. Pins: `nested_alter_column_type_on_a_missing_table_is_table_or_view_not_found`, `test_nested_alter_column_type_on_a_missing_table_matches_spark` (`missing-namespace-bigint`). |

### Round 4 out-of-scope observation

- R-U5-DECIMAL-DEFAULT: `CREATE TABLE … (a DECIMAL)` (also `NUMERIC`, `DEC`) creates `decimal(38,18)` in RePark, because the shared `create_table::decimal_from_info` maps a bare type to (38,18). Spark 4.1.2 creates `decimal(10,0)` (measured `create_dec`/`desc_cd`: `DESCRIBE TABLE` gives `["a", "decimal(10,0)", null]`). The same lowering serves top-level ALTER. Only the nested TYPE route decides the Spark default (C-035).
- R-U5-NS-LEAK: the top-level route `ALTER TABLE sc.nope.t ALTER COLUMN id TYPE BIGINT` still leaks `NamespaceNotFound => No such namespace: NamespaceIdent(["nope"])`. Spark gives `[TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`nope`.`t` cannot be found. Verify the spelling and correctness of the schema and catalog. If you did not qualify the name with a schema, verify the current_schema() output, or qualify the name with the correct schema and catalog. To tolerate the error on drop use DROP VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01` (measured `ns_missing_top`). Pre-existing on the top-level route.
- The round-3 residues above carry forward unchanged.

## WO U5 PR2a (ALTER COLUMN COMMENT, 2026-09-24)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-037 | `ALTER TABLE t ALTER [COLUMN] <path> COMMENT '<doc>'` and `CHANGE [COLUMN] <path> COMMENT '<doc>'` set the Iceberg field `doc` for a top-level column or a nested struct field, in one schema update for a comma list of COMMENT specs. `''` stores `""`, a double-quoted literal is the doc, a backslash-escaped quote is unescaped, and a trailing `;` is accepted. A COMMENT on a map value or list element succeeds as a no-op, as in Spark: no doc, no new schema, and a list naming only such targets commits nothing (narrowed in round 3, C-045). The residual `ALTER COLUMN … COMMENT is not supported yet` refusal is gone, and `dbt-repark` inherits `spark__alter_column_comment`, so `persist_docs.columns` lands each description (`DBT-COLCOMMENT-1` retired). | Spark 4.1.2 + Iceberg 1.11.0 probes `target/probe-u5-pr2/spark.out` (`ac_*`) and `spark2.out`; Rust, facade and dbt pins; scoreboard replay of `D-ALTER-COMMENT`. | PROVEN | Spark: `data` doc `new doc`, `st.x` `nested doc`, `id` `""`, `CHANGE COLUMN cat` `via change`, `"dq doc"`, bare CHANGE/ALTER and the two-spec list land; `m.value`/`arr.element` succeed with no JSON doc. Pins: `crates/repark-spark/src/tests/column_comment_ddl.rs`, `python/repark/tests/test_ice_ddl_alter_2.py`, `python/dbt-repark/tests/test_statement_surface.py` `S-COLUMN-COMMENT`/`S-COLUMN-COMMENT-CHANGE`, `test_gold_models.py::test_column_documentation_sets_the_column_docs`. Replay: `D-ALTER-COMMENT` EQUAL. |
| C-038 | The COMMENT refusals answer Spark's text with the schema unchanged: an unknown column or nested field raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703), a step under a primitive `INVALID_FIELD_NAME` (42000), a map key `Unsupported table change: Cannot update map keys: map<k, v>` (a field under a map key: C-044), a missing table `TABLE_OR_VIEW_NOT_FOUND` (42P01). `COMMENT NULL`, `COMMENT 5`, a double-quoted column name and top-level `TYPE <t> COMMENT` raise `PARSE_SYNTAX_ERROR` near the token, a missing literal `… near end of input`, and a token after a complete spec list `… near '<t>': extra input '<t>'` (42601) when it is the last token (narrowed in round 3, C-047). | Spark probes plus Rust and facade pins. | PROVEN | Measured: `ac_3`, `ac_7`, `prim_parent`, `mkey`, `ac_11`, `ac_5`, `num`, `dq_ident`, `ac_10`, `no_str`, `first`. Spark appends `; line 1 pos 0` to analysis errors; RePark's analysis texts end at the SQLSTATE, as on the PR1 routes. |
| C-039 | Near misses keep their routes: top-level `ALTER COLUMN id TYPE BIGINT`, the hive `CHANGE COLUMN a a <type> [COMMENT …]`, `ALTER COLUMN … DROP NOT NULL`, `ALTER COLUMN … FIRST` and nested `ALTER COLUMN <path> TYPE`. | Rust near-miss test plus the full `repark-spark` lib suite. | PROVEN | `alter_column_comment_leaves_type_and_hive_change_forms_unchanged`, `test_alter_column_type_and_hive_change_stay_unchanged`, `tests::nested_column_ddl`, `router::hive_change_column` in-module tests. |

### PR2a out-of-scope observation

- A column list that mixes `COMMENT` with another change (`data COMMENT 'a', cat TYPE STRING`) is accepted by Spark's `alterColumnSpecList` and refused here as not implemented (UnsupportedOperationException, named text). Loud. Measured and named in round 2 as R-U5-MIXED-COMMENT-LIST.
- The native (ANSI) door has no column-comment statement (`COMMENT ON COLUMN`). Not added; the Spark spelling is the parity cell.
- `D-ADD-COL-STRUCT`, `D-X-ADD-COL-MAP-KEY-STRUCT` and `D-CREATE-PART-TRUNC-BIN` fail in the fork, not RePark (the arrow null-column fill has no MAP/LIST arm; the partition-value builder appends a `BinaryBuilder` where the schema made a `LargeBinary`). They are recorded as fork questions in the hand-back.

## WO U5 PR2a round 2 (critic r1 V-001..V-006, 2026-09-24)

Measured on Spark 4.1.2 + Iceberg 1.11.0: `target/probe-u5-pr2a-r1fix/spark.out` (critic
shapes and the spec-list sweep), `spark2.out` (bare `ALTER`/`CHANGE`, action tails, list
tails), `spark3.out`/`spark4.out` (quoted names on every nested route). RePark before and after
this round: `repark-before.out`, `repark*-after.out` in the same directory.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-040 | A `COMMENT` spec list whose resolved paths repeat a column, or name a column and a field under it (struct field, `element`, `value`), raises AnalysisException `[NOT_SUPPORTED_CHANGE_SAME_COLUMN] ALTER TABLE ALTER/CHANGE COLUMN is not supported for changing `<catalog>`.`<ns>`.`<table>`'s column <shorter path> including its nested fields multiple times in the same command. SQLSTATE: 0A000` with the schema unchanged. Paths compare after case-insensitive resolution, so `Data`/`data` and `` `data` ``/`data` repeat. The table is always named in three parts. An unresolved path in the same list wins over the repeat. Siblings (`st.x`, `st.y`) and three distinct specs commit. | Spark probe plus Rust and facade pins; mutation. | PROVEN | Measured `dup`, `dupcase`, `dupcase2`, `dupnest`, `dupnestrev`, `dupnestcase`, `dupleaf` (`` `st`.`x` ``), `threedup`, `arrdup`, `arrelemdup` (`` `arr`.`element` ``), `mapdup`, `changedup`, `changedup2`, `b_dup`, `bt_dup`, `bt_nest_dup`, `twopart`/`onepart` (still `` `sc`.`ns`.`ac` ``); `missdup`/`missthendup`/`dupthenmiss` give UNRESOLVED_COLUMN; `siblings`, `prefixname`, `three` commit. Before this round each repeat committed, last spec winning. Pins: `column_comment_ddl.rs::alter_column_comment_refuses_a_repeated_column_like_spark`, `test_ice_ddl_alter_2.py::test_alter_column_comment_refusals_match_spark` (`repeated-column`, `repeated-column-case`, `repeated-parent-and-field`). Mutation: with the check disabled the Rust test goes red at `ALTER COLUMN data COMMENT 'dup', data COMMENT 'dup2'` (the statement succeeds). |
| C-041 | `ALTER [COLUMN] <path>` or bare `CHANGE <path>` followed by `SET NOT NULL`, `DROP NOT NULL`, `SET DEFAULT <expr>`, `DROP DEFAULT`, `FIRST` or `AFTER <col>` and then `COMMENT` raises ParseException `[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601`, top level or nested. A single-quoted column name on any nested-column route raises `PARSE_SYNTAX_ERROR` near the quoted token, or near `'.'` when the quoted part follows a period. | Spark probe plus Rust and facade pins. | PROVEN | Measured `snn`, `dnn`, `sdef`, `ddef`, `sdefnum`, `stsnn`, `chsdef`, `first`, `after`, `nested_first`, `b_snn`, `b_dnn`, `b_sdef`, `b_ddef`, `c_snn_bare`; `sq_first`, `sq_list`, `sq_change` (`''data''`/`''cat''`), `sq_add2`, `sq_drop`, `sq_type` (`''st''`), `sq_add`, `sq_rename` (`'.'`). Before this round the action forms leaked `SQL error: ParserError("Expected: end of statement, found: COMMENT …")` and the quoted names committed silently (`sq_first`, `sq_list`, `sq_change`, `sq_add`, `sq_drop`). Pins: `alter_column_comment_refuses_malformed_forms_like_spark`, `single_quoted_column_names_are_parse_errors_like_spark`, facade ids `set-not-null-then-comment`, `drop-not-null-then-comment`, `set-default-then-comment`, `first-then-comment`, `single-quoted-name`. |
| C-042 | A later spec in a `COMMENT` list that is not `COMMENT` is decided by its next token. `TYPE`/`SET`/`DROP`/`FIRST`/`AFTER` stay the mixed-list refusal (C-043). End of input, `,` or `;` raise ParseException `Operation not allowed: ALTER TABLE table ALTER COLUMN requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER.`, rendered through the shared `SQL error: ParserError("…")` wrapper (R-U5-OP-NOT-ALLOWED-WRAP). Any other token raises `[PARSE_SYNTAX_ERROR] Syntax error at or near '<t>': extra input '<t>'. SQLSTATE: 42601` when it is the last token, and the plain `near '<t>'` when more follow (round 3, C-047). A path that does not parse raises `PARSE_SYNTAX_ERROR` near its token, or `near end of input` after a trailing comma. | Spark probe plus Rust and facade pins. | PROVEN | Measured `garb`, `garbstr` (`''x''`), `rparen` (`')'`), `nestgarb`, `bare`, `bare_semi`, `bare3`, `nestbare`, `numpath` (`'5'`), `trailcomma`. Before this round each answered the mixed-list not-implemented text. Pins: `alter_column_comment_refuses_malformed_forms_like_spark`, facade ids `list-extra-input`, `list-spec-without-action`. |
| C-043 | A two-spec `COMMENT` list adds exactly one schema and one metadata file. A list mixing `COMMENT` with `TYPE`, `FIRST`, `AFTER`, `SET`/`DROP` refuses UnsupportedOperationException `This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with another change for `<path>` in one column list; only a list of COMMENT changes is supported, so split the statement` with the schema unchanged. | Rust and facade pins. | PROVEN | `alter_column_comment_takes_bare_keywords_and_a_spec_list_like_spark` asserts `schemas_iter().count()` rises by 1; `alter_column_comment_refuses_a_repeated_column_like_spark` asserts one schema for a four-spec list; `test_alter_column_comment_takes_the_dbt_statement_shapes` counts one new metadata file. Mixed pins: Rust `cat TYPE STRING`, `cat FIRST`, `cat AFTER id`, `cat DROP NOT NULL`, `st.x TYPE BIGINT`; facade `mixed-list-type`, `mixed-list-first`. Spark accepts these lists: R-U5-MIXED-COMMENT-LIST. |

### Round 2 residues

- R-U5-MIXED-COMMENT-LIST: Spark 4.1.2 accepts a `COMMENT` list mixed with another action. `data COMMENT 'm1', cat TYPE STRING` returns OK and lands `data` doc `m1` (`mixtype`). `data COMMENT 'm2', cat FIRST` moves `cat` first and lands the doc (`mixfirst`). `cat AFTER id` (`mixafter`), `cat DROP NOT NULL` (`mixdnn`) and `st.x TYPE BIGINT` (`mixtype_nested`) return OK. `cat SET NOT NULL` raises AnalysisException `Cannot change nullable column to non-nullable: cat.` (`mixsnn`). `cat TYPE INT` raises `[NOT_SUPPORTED_CHANGE_COLUMN] … `cat` with type "STRING" to `cat` with type "INT". SQLSTATE: 0A000` (`mixtypebad`). `data FIRST` in the same list raises NOT_SUPPORTED_CHANGE_SAME_COLUMN (`mixdup`), a missing column raises UNRESOLVED_COLUMN (`mixmissing`), and `SET DEFAULT`/`DROP DEFAULT` raise UnsupportedOperationException `Cannot apply unknown table change: …UpdateColumnDefaultValue@…` (`mix_sdef`, `mix_ddef`). RePark refuses every one with the C-043 not-implemented text. This is loud, never a silent accept. Matching needs the TYPE, move and nullability routes to share one schema update with the doc changes, which is more than one arm.
- R-U5-SPEC-LIST-NOT-COMMENT-FIRST (reworded in the round 4 fold): the leak was not pre-existing. At base `6cf215bd` the I6 residual refusal `refuse_unsupported_alter_sql` ran after the ALTER intercepts and answered any unclaimed statement holding `ALTER COLUMN` and `COMMENT` with the named NotImplemented text. This PR deleted it, and from round 2 to round 3 every list whose first spec is not `COMMENT` leaked raw sqlparser text. Examples are `cat TYPE STRING, data COMMENT 'm8'`, `id DROP NOT NULL, data COMMENT 'dn'` and `id SET NOT NULL, …`, which gave `SQL error: ParserError("Expected: ADD, RENAME, PARTITION, SWAP, DROP, REPLICA IDENTITY, SET, or SET TBLPROPERTIES after ALTER TABLE, found: data …")`. `ALTER TABLE IF EXISTS … ALTER COLUMN id COMMENT 'ie'` and `… PARTITION (id=1) ALTER COLUMN id COMMENT 'pp'` leaked the same way (critic r3 V-001; `target/probe-u5-pr2a-r3fold/repark-before.out`). Since the round 4 fold, these lists answer the C-043 mixed-list text (C-049), the wrappers answer Spark's `PARSE_SYNTAX_ERROR` (C-050), and every other unclaimed `ALTER COLUMN … COMMENT '<doc>'` statement answers a named residual refusal (C-051). Spark returns OK for most of these lists and lands the doc (R-U5-MIXED-COMMENT-LIST, round 4 table). A list with no `COMMENT` (`id DROP NOT NULL, cat DROP NOT NULL`, `id TYPE BIGINT, cat TYPE STRING`) is outside the deleted refusal's reach. Spark returns OK, and RePark still leaks the raw `Expected: ADD, RENAME, … found: cat` text on the stock route. The nested `st.x TYPE BIGINT, st.y TYPE BIGINT` gives `[PARSE_SYNTAX_ERROR] Syntax error at or near ','` on the PR1 route. Both behaviours predate this PR.
- R-U5-OP-NOT-ALLOWED-WRAP: `repark_core::error_map::spark_parse_message` unwraps a `ParserError` message only when it starts with `[`. Spark's `Operation not allowed: …` ParseExceptions therefore surface as `SQL error: ParserError("Operation not allowed: …")`. This holds here (C-042), on `ALTER NAMESPACE … SET DBPROPERTIES` (`Values must be specified for key(s)`) and on `CREATE TEMPORARY VIEW … TBLPROPERTIES`. The one-line fix is in `repark-core`, and it re-pins those two routes.
- R-U5-ENGINE-PREFIX: the map-key refusal `ALTER COLUMN m.key COMMENT 'mk'` surfaces as `datafusion engine error: Execution error: Unsupported table change: Cannot update map keys: map<string, int>`, where Spark raises SparkException `Unsupported table change: Cannot update map keys: map<string, int>` (`target/probe-u5-pr2/spark2.out` `mkey`). This follows the PR1 precedent for the nested TYPE route (`test_u5_alter_ddl.py` `_unsupported`). The prefix strip belongs to whichever unit strips it for the PR1 route.
- R-U5-CHANGE-COLUMN-ACTION: `CHANGE COLUMN <col>` followed by `SET NOT NULL`, `DROP NOT NULL`, `DROP DEFAULT` or `FIRST` and then `COMMENT` raises ParseException `Renaming column is not supported in Hive-style ALTER COLUMN, please run RENAME COLUMN instead.` in Spark (`chsnn`, `c_dnn`, `c_ddef`, `c_first`). `CHANGE COLUMN data SET DEFAULT 'a' COMMENT 'x'` raises `PARSE_SYNTAX_ERROR` near `'COMMENT'` (`c_sdef`). `CHANGE COLUMN data SET NOT NULL` raises AnalysisException `Cannot change nullable column to non-nullable: data.` (`c_snn_nocomment`). RePark's hive CHANGE route answers AnalysisException `[PARSE_SYNTAX_ERROR] Syntax error at or near 'SET'` (or `'DROP'`/`'FIRST'`) for each. This is loud and pre-existing on `router/hive_change_column.rs`, and C-041 leaves it alone.

## WO U5 PR2a round 3 (critic r2 V-001..V-006, 2026-09-24)

Measured on Spark 4.1.2 + Iceberg 1.11.0: `target/probe-u5-pr2a-r2fix/spark.out` (map keys,
element/value, order, tails, name rendering, the TYPE sweep and `USE`), `spark2.out` (tail
calibration), `spark3.out` (list tails). RePark after this round: `repark.out`, `repark3.out`,
compared by `cmp.py`. Every shape is EQUAL except the field id in the map rendering
(R-U5-MAP-KEY-FIELD-IDS).

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-044 | A COMMENT or nested TYPE path strictly under a map key raises `Unsupported table change: Cannot alter map keys: <map>` with the schema unchanged, alone or in a list. The map renders in Iceberg's type form with field ids. The key itself keeps `Cannot update map keys`. On TYPE, the same-type target is a no-op (no schema), a non-promotable target raises `Cannot change column type: mm.key.k: int -> string`, and a Spark-only target raises `NOT_SUPPORTED_CHANGE_COLUMN` before the key check. | Spark probe plus Rust and facade pins; mutation. | PROVEN | Measured `mmkeyfield`, `mmkeyfield_list`, `t_mmkey_bigint`, `t_mmkey_same`, `t_mmkey_string`, `t_mmkey_smallint`. Before this round the COMMENT and TYPE forms committed (critic `repark.out` `mmkeyfield`, `repark2.out` `mmkey_type`). Pins: `column_comment_ddl.rs::alter_column_comment_refuses_map_keys_in_spark_order`, `test_map_key_changes_refuse_in_spark_order` (`map-key-field`, `map-key-field-in-list`, `map-key-field-type`). Mutation: with the under-key step dropped, the Rust test goes red at `ALTER COLUMN mm.key.k COMMENT 'k'` (the statement succeeds). |
| C-045 | `ALTER COLUMN m.value COMMENT …`, `arr.element COMMENT …`, `mm.value COMMENT …` and a list of only such specs return OK and add no schema. In a list with a real column, only the column's doc commits (one schema). A struct field under a value or element (`m.value.z`, `arr.element.a`) lands its doc. | Spark probe plus Rust and facade pins. | PROVEN | Measured `mval`, `arrel`, `mmval`, `mval_arrel` (unchanged), `mval_data` (+1), `mvalfield`, `arrelfield` (+1). Before this round each added a structurally identical schema. Pins: `alter_column_comment_sets_the_iceberg_doc_like_spark` (schema count), `alter_column_comment_refuses_map_keys_in_spark_order`, `test_element_and_value_comments_add_no_schema_like_spark`. |
| C-046 | In a COMMENT list, an unresolved path wins over a repeat, and a repeat wins over a map-key refusal. Among map-key refusals, the first map in the post-order schema visit wins, whatever the spec order. | Spark probe plus Rust and facade pins. | PROVEN | Measured `mkeydup` (`` `m`.`key` ``), `mmkeyfield_dup` (`` `mm`.`key` ``), `mkeythenmiss`, `mmkeyfield_miss` (UNRESOLVED `nope`), `mkey_then_mmkeyfield`, `mmkeyfield_then_mkey`, `mkey_mval` (all `Cannot update map keys: map<string, struct<…>>`). Pins: `alter_column_comment_refuses_map_keys_in_spark_order`, `test_map_key_changes_refuse_in_spark_order` (`repeated-map-key`, `map-key-then-missing`, `map-keys-in-schema-order`). |
| C-047 | A token after a complete COMMENT spec, or after a list path, raises `[PARSE_SYNTAX_ERROR] Syntax error at or near '<t>': extra input '<t>'. SQLSTATE: 42601` only when it is the last token before end of input or `;`. When more tokens follow, it raises `… near '<t>'. SQLSTATE: 42601`. | Spark probe plus Rust and facade pins. | PROVEN | Plain: `tail_type`, `tail_set_nn`, `tail_drop_nn`, `tail_after`, `tail_set_default`, `tail_drop_default`, `tail_comment`, `tail_not_null`, `list_tail_type`, `list_tail_drop` and the calibration run (`TYPE foo`, `FIRST foo`, `AFTER 5`, `NOT NULL foo`, …), `tail_word_word`, `list_word_word`. Extra input: `tail_first`, `tail_set`, `tail_drop`, `tail_type_bare`, `tail_word`, `tail_number`, `tail_rparen`, `tail_word_semi`, `list_word_semi`, `AFTER`, `NOT`, `NULL`. Pins: `alter_column_comment_tails_follow_spark_token_recovery`, facade `trailing-type-action`, `trailing-drop-action`, `trailing-bare-type`. |
| C-048 | `UNRESOLVED_COLUMN.WITH_SUGGESTION` renders the name from its parsed parts (`` `st.x` `` for a backquoted part, `` `st`.`x.y` ``) and each suggestion split on `.` (`` `p`.`q` ``), on the COMMENT, nested TYPE and nested ADD routes, and on the top-level MOVE route (`ALTER COLUMN <c> FIRST`/`AFTER`, which shares `column_move::unresolved_column`; named in the round 4 fold). After `USE sc.ns`, a repeat on `ac` or `ns.ac` names `` `sc`.`ns`.`ac` ``. | Spark probe plus Rust and facade pins. | PROVEN | Measured `dotq_missing`, `nested_dotq_missing`, `nested_type_missing`, `nested_type_dotq_missing`, `add_parent_missing`, `use_one_part_repeat`, `use_two_part_repeat`. Pins: `unresolved_columns_render_backquoted_parts_like_spark`, `a_repeated_column_after_use_renders_the_three_part_table_like_spark`, `test_unresolved_columns_render_backquoted_parts_like_spark`, `test_a_repeated_column_after_use_names_the_three_part_table`. MOVE route: Spark `move_nope` (`target/probe-u5-pr2a-r3fold/spark2.out`) gives `` [UNRESOLVED_COLUMN.WITH_SUGGESTION] … name `nope` … [`id`, `data`, `cat`, `st`, `arr`, `m`, `d`, `p`.`q`, `mm`]. SQLSTATE: 42703 ``, RePark renders the same list, pinned by the facade param `move-missing-column`. |

### Round 3 residues

- R-U5-MAP-KEY-FIELD-IDS: `CREATE TABLE … (mm MAP<STRUCT<k: INT>, INT>)` numbers the key struct's child before the value in RePark (key 21, `k` 22, value 23), where Spark numbers the value first (key 21, value 22, `k` 23). The map-key refusal text therefore renders `map<struct<22: k: optional int>, int>` where Spark renders `map<struct<23: k: optional int>, int>` for the same DDL. The pins read the id from the table. This is pre-existing in CREATE TABLE field-id assignment, not in this route.
- R-U5-DOTTED-TOP-TYPE: `ALTER COLUMN `st.x` TYPE BIGINT` (one backquoted part) commits in RePark and widens the nested `st.x`. Spark raises AnalysisException `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `st.x` cannot be resolved. Did you mean one of the following? [`id`, `data`, `cat`, `st`, `arr`, `m`, `d`, `p`.`q`, `mm`]. SQLSTATE: 42703` (critic `spark2.out` `dotq_type`). The one-part path falls through to the pre-existing top-level TYPE route, which joins the name before Iceberg resolves it.

## WO U5 PR2a round 4 fold (critic r3 V-001, V-002, 2026-09-24)

Measured on Spark 4.1.2 + Iceberg 1.11.0 in `target/probe-u5-pr2a-r3fold/`. `spark.out` holds
the critic shapes and the sweep (`ADD COLUMNS … COMMENT`, `CHANGE COLUMN … AFTER`),
`spark2.out` the first-spec and wrapper variants plus `move_nope`, `spark3.out` the bare
`ALTER`/`CHANGE` lists, and `spark4.out` the residual shapes. RePark before this fold is in
`repark*-before.out` and `repark4-mid.out`, and after it in `repark*-after.out`. Compare them
with `cmp.py`.

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-049 | A column list whose first spec is not `COMMENT` and that holds a later `<path> COMMENT '<doc>'` spec refuses with the C-043 mixed-list text, naming the first spec's path, with the schema unchanged. The first spec may be `TYPE`, `SET`/`DROP NOT NULL`, `SET`/`DROP DEFAULT`, `FIRST` or `AFTER`, top level or nested, after `ALTER COLUMN`, bare `ALTER` or bare `CHANGE`. If that later spec has no literal, the statement raises `[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601`. A token after its literal raises `extra input` under the C-047 rule, and a single-quoted path raises `near` its token. A first spec with no action raises Spark's `Operation not allowed: … requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER.` before a `,`, and `PARSE_SYNTAX_ERROR` near its first token otherwise — measured and pinned for a bare-word first spec (`id foo, …` → near 'foo'); a first spec whose first token is `SET` or `DROP` names the NEXT token on Spark and is a residue, see R-U5-RESIDUAL-COMMENT-SHAPES. | Spark probe plus Rust and facade pins; mutation. | PROVEN | Measured `dnn_first`, `snn_first`, `sdef_first`, `ddef_first`, `first_first`, `after_first`, `type_first`, `ntype_first`, `dnn_type_comment`, `dnn_nested_comment`, `alter_dnn_first`, `bare_alter_type_list`, `bare_change_type_list`, `bare_change_dnn_list`, `bare_alter_ntype_list`, `type_struct_first`, `dnn_two_comments` (mixed-list text). `dnn_nolit`, `type_first_nolit`, `dnn_comment_tail`, `dnn_comment_first_after`, `dnn_quoted_later`, `bare_first`, `bare_alter_bare_list`, `word_first` and `not_null_first` are EQUAL to Spark. Before the fold, each leaked raw sqlparser text. The exceptions: `first_first`/`after_first` answered the MOVE route's `` trailing tokens … (starting at `,`) ``, `ntype_first` answered `near ','`, and the bare `CHANGE` lists answered the hive route's `near 'TYPE'`/`'DROP'`. Pins: `column_comment_ddl.rs::a_comment_list_after_another_change_is_the_mixed_list_refusal`, `wrapped_and_malformed_comment_lists_are_parse_errors_like_spark`; facade `mixed-list-drop-not-null-first`, `mixed-list-type-first`, `mixed-list-missing-literal`. Mutation: with `comment_list_after_first_spec` returning `Ok(None)`, the Rust test goes red at `ALTER TABLE ice.sales.ac ALTER COLUMN id DROP NOT NULL, data COMMENT 'dn'` (`column_comment_ddl.rs:374`), and `wrapped_and_malformed_comment_lists_are_parse_errors_like_spark` goes red too. |
| C-050 | `ALTER TABLE IF EXISTS <t>` followed by an `ALTER`/`CHANGE` column form with a `COMMENT` raises `[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601`, with the token as spelled (`'exists'`), whether or not the table exists and even with a `PARTITION` spec. `ALTER TABLE <t> PARTITION (…) ALTER …` with a `COMMENT` raises `… near 'ALTER'` (`'alter'`). No schema changes. | Spark probe plus Rust and facade pins. | PROVEN | Measured `ie`, `ie_change`, `ie_missing`, `ie_nested`, `ie_mixed`, `ie_lower`, `ie_pp`, `pp`, `pp_alter`, `pp_nested`, `pp_lower`, all EQUAL. Before the fold, each leaked a raw ParserError (`Expected: SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN, found: COMMENT …`, `Expected: RENAME, found: ALTER …`). Pins: `wrapped_and_malformed_comment_lists_are_parse_errors_like_spark`, `test_wrapped_column_comments_are_parse_errors_like_spark` (`if-exists`, `if-exists-lower`, `partition-spec`, `partition-spec-nested`). |
| C-051 | Some `ALTER TABLE` statements hold `ALTER COLUMN` followed later by `COMMENT '<literal>'`, and no intercept claims them. Each one refuses with UnsupportedOperationException `This feature is not implemented: ALTER TABLE … ALTER COLUMN … COMMENT is supported only as ALTER TABLE <table> ALTER COLUMN <column> COMMENT '<doc>' or a list of such COMMENT specs; this statement shape is not supported`. The refusal is `residual_column_comment_refusal`, run where the deleted I6 refusal ran. It is token-based. A column named `comment` with `TYPE`, `ADD COLUMN … COMMENT` and the hive `CHANGE COLUMN … COMMENT` are outside it. | Spark probe plus Rust and facade pins. | PROVEN | Measured `add_then_alter`, `drop_then_alter`, `dnn_word_comment` and `type_missing_list`. Before the fold, each leaked raw text (`repark4-mid.out`). `comment_named_type` still returns OK, as in Spark. Pins: `other_column_comment_statements_answer_the_residual_refusal` (the four shapes, plus the three `is_none` near misses), `test_other_column_comment_statements_answer_the_residual_refusal`. Spark's texts: R-U5-RESIDUAL-COMMENT-SHAPES. |

### Round 4 fold residues

- R-U5-MIXED-COMMENT-LIST (extended): the C-049 lists behave like the round 2 lists. Spark returns OK and lands the doc for `dnn_first` (doc `dn`), `first_first` (moves `cat` first), `after_first`, `type_first`, `ntype_first`, `dnn_type_comment`, `dnn_nested_comment`, `alter_dnn_first`, the bare `ALTER`/`CHANGE` lists, `type_struct_first` and `dnn_two_comments`. `snn_first` raises AnalysisException `Cannot change nullable column to non-nullable: id.`. `sdef_first`/`ddef_first` raise UnsupportedOperationException `Cannot apply unknown table change: org.apache.spark.sql.connector.catalog.TableChange$UpdateColumnDefaultValue@…`. `type_dec_first` raises SparkException `Unsupported table change: Cannot change column type: id: long -> decimal(20, 0)`. RePark answers the mixed-list text for each. This is loud and never a silent accept.
- R-U5-RESIDUAL-COMMENT-SHAPES: Spark answers `[PARSE_SYNTAX_ERROR] Syntax error at or near ','. SQLSTATE: 42601` for `ADD COLUMNS (z2 INT), ALTER COLUMN id COMMENT 'aa'` and for `ALTER COLUMN id TYPE, data COMMENT 'x'`. It answers `… near 'COLUMN'` for `DROP COLUMN cat, ALTER COLUMN id COMMENT 'da'` and `… near 'foo'` for `ALTER COLUMN id DROP NOT NULL foo COMMENT 'x'`. RePark answers the C-051 named refusal. Round 4 scoped critic (measured, /tmp probes sp5.out): `ALTER COLUMN data SET DATA TYPE STRING, cat COMMENT 'x'` — Spark `[PARSE_SYNTAX_ERROR] Syntax error at or near 'DATA'. SQLSTATE: 42601`, RePark `… near 'SET' …` (same condition and SQLSTATE, a different token; `RENAME TO id2, data COMMENT 'x'` is EQUAL, near 'RENAME'); `ALTER COLUMN id SET DATA TYPE BIGINT COMMENT 'x'` — Spark near 'DATA', RePark the C-051 named refusal. Fix for both: when the first unmatched token is SET or DROP, name the next token.
- R-U5-ADD-COLUMNS-TRAILING-COMMENT (silent accept, pre-existing): `ALTER TABLE sc.ns.ac ADD COLUMNS (z INT) COMMENT 'ac'` commits `z` in RePark and ignores the trailing `COMMENT`. Spark raises ParseException `[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601` (`add_then_comment`). The statement holds no `ALTER COLUMN`, so the deleted I6 refusal never reached it. It is outside this fold.
- R-U5-IF-EXISTS-ALTER-TABLE (pre-existing): Spark rejects every `ALTER TABLE IF EXISTS` with `[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS'. SQLSTATE: 42601`. `ALTER TABLE IF EXISTS sc.ns.ac ADD COLUMN z INT COMMENT 'x'` commits `z` in RePark, a silent accept (`ie_add`). `ALTER TABLE IF EXISTS sc.ns.ac ALTER COLUMN st.x TYPE BIGINT` leaks `SQL error: ParserError("Expected: SET/DROP NOT NULL, SET DEFAULT, or SET DATA TYPE after ALTER COLUMN, found: . …")` (`ie_ntype`). Only the COMMENT-bearing column forms answer `near 'EXISTS'` (C-050).
- R-U5-PARTITION-CHANGE (pre-existing): `PARTITION (id=1) CHANGE COLUMN id COMMENT 'pc'` raises `[PARSE_SYNTAX_ERROR] Syntax error at or near ''pc'': extra input ''pc''. SQLSTATE: 42601` in Spark. `PARTITION (id=1) CHANGE COLUMN id id BIGINT COMMENT 'ph'` raises `[INVALID_STATEMENT_OR_CLAUSE] The statement or clause: ALTER TABLE ... PARTITION ... CHANGE COLUMN is not valid. SQLSTATE: 42601`. `PARTITION (id=1) ALTER COLUMN id TYPE BIGINT` raises `… near 'ALTER'`. RePark leaks `SQL error: ParserError("Expected: RENAME, found: CHANGE …")` (`… found: ALTER …` for the TYPE form).
- R-U5-CHANGE-COLUMN-ACTION (extended): the hive `CHANGE COLUMN cat cat STRING COMMENT 'ca' AFTER id` returns OK in Spark, landing doc `ca` and moving `cat` after `id` (`change_hive_comment_after`). `CHANGE COLUMN cat cat STRING AFTER id COMMENT 'cc'` raises `[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601`. RePark answers AnalysisException `` Error during planning: trailing tokens after ALTER TABLE CHANGE COLUMN `cat` (starting at `AFTER`) `` for both. `CHANGE COLUMN id DROP NOT NULL, data COMMENT 'cdn'` returns OK in Spark, and RePark answers `[PARSE_SYNTAX_ERROR] Syntax error at or near 'DROP'`. These are loud and pre-existing on `router/hive_change_column.rs`. The bare `CHANGE COLUMN cat COMMENT 'cb' AFTER id` and `ALTER COLUMN cat COMMENT 'cd' AFTER id` are EQUAL to Spark (`near 'AFTER'`).

## WO U5 PR2b (v1 create, transform write order, branch on an empty table; round 2 critic r1 V-001..V-007, 2026-09-25)

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-052 | `CREATE TABLE … TBLPROPERTIES ('format-version'='1')`, the CTAS form, `'01'` and the ANSI `WITH (format_version = 1)` create a format v1 table: no `last-sequence-number`, no snapshot `sequence-number`, the legacy `schema`/`partition-spec` keys, a snapshot-log entry and `main` after the seed; DELETE is copy-on-write. `'5'` raises IllegalArgumentException `Unsupported format version: v5 (supported: v4)`, `'abc'` raises `For input string: "abc"`, and `0`/`-1`/`4` refuse as not implemented. | Spark probe plus Rust and facade pins; scoreboard replay. | PROVEN | Spark 4.1.2 + Iceberg 1.11.0 `target/probe-u5-pr2b/spark.out`. Pins: `create_format_version_one.rs`, `v3/create.rs::format_version_one_creates_v1_and_deletes_copy_on_write`, `test_ice_ddl_alter_2.py::test_create_format_version_one_writes_v1_like_spark`, `test_create_format_version_refusals_match_spark`. Replay `D-CREATE-V1`: equal except `md.refs` (R-U5-PR2B-V1-REFS). Supersedes the archived `v3-2-create-v3-opt-in` C-007 (errata note there). |
| C-053 | No path writes a non-main ref into format v1 metadata. Every such path refuses before it writes, through `repark_iceberg::write::refuse_ref_write_on_format_v1`, with UnsupportedOperationException `This feature is not implemented: <KIND> on the format v1 table <ns>.<table> is not supported: the Iceberg fork writes v1 metadata without its refs, so the new ref would be lost`, where `<KIND>` is `BRANCH` or `TAG`. The paths are branch/tag DDL on both doors (CREATE, IF NOT EXISTS, CREATE OR REPLACE, REPLACE, AS OF VERSION, `main` with retention), a session WAP branch write (seeded or empty table), `INSERT`/`DELETE` into a `t.branch_x` selector (belt-and-braces on this branch, see round 4), every RePark branch commit through `commit_target::maybe_to_branch`, `CALL fast_forward` to a new branch and `rewrite_data_files(branch => …)`. A write into a missing `t.branch_x` answers REF-1's `Cannot use branch (does not exist): x` before the kernel (round 3: `INSERT`, `DELETE`, `MERGE`, `INSERT OVERWRITE`), as Spark does on v1. `writeTo().option('branch', …)` is refused by the Python facade before the kernel (R-U5-PR2B-WRITETO-BRANCH-OPTION). Commits that move only `main` keep working. | Rust pins per door, red-first under a neutralised kernel; one facade pin. | PROVEN | Critic r1 V-001 (`/tmp/xrv-probe/p3.py`, `p2.py`), V-005 (`p1.py`). Red-first (`target/probe-u5-pr2b-r1fix/red-first.txt`, kernel forced to `Ok`): the WAP write on a seeded and an empty table, `fast_forward('sales.t', 'nb', 'main')`, CREATE BRANCH on an empty v1 table, native `CREATE BRANCH audit` and the kernel's `create_branch_on_empty_table` all report success; `INSERT INTO t.branch_b1` already refused (`Cannot use branch (does not exist): b1`). Pins: `crates/repark-iceberg/src/tests/v1_ref_writes.rs`, `crates/repark-spark/src/tests/v1_ref_writes.rs`, `ref_branch_on_empty.rs::branch_and_tag_on_a_v1_table_refuse_until_the_fork_keeps_v1_refs`, `v3/create.rs::ref_ddl_on_a_v1_table_refuses_before_the_ref_is_lost`, `test_a_wap_branch_write_on_a_v1_table_refuses_and_writes_no_ref`. Round 3 (critic r2 V-001, V-002): the head-362b73d2 kernel ran before the missing-branch check, so `INSERT`/`MERGE INTO t.branch_b` on v1 answered the kernel text where Spark answers `Cannot use branch (does not exist): b`; the check now runs first. Re-measured 2026-09-25 (`scratchpad/probe/spark.out` `insb`, `mergeb`, `delb`, `updb`, `iob`, `insbe`). Pin: `v1_ref_writes.rs::writes_into_a_missing_branch_on_a_v1_table_answer_the_missing_branch_text`. Round 4 (critic r3 V-004, dated 2026-09-25): the v1 reader drops every non-main ref until F-V1-REFS-1 lands (repin repark#836), so an existing-branch selector write on v1 cannot occur on this branch. Measured by the verifier (`scratchpad/probe/v1reg.py`, `v1reg.out`): a v1 metadata file carrying refs `main`, `b` (branch) and `t` (tag) registered through `CALL sc.system.register_table` lists `main` only in the refs metadata table, `SELECT * FROM v1r.branch_b` raises `Cannot find matching snapshot ID or reference name for version b`, and `INSERT`, `DELETE`, `UPDATE`, `MERGE` and `INSERT OVERWRITE` into `v1r.branch_b` all answer `Cannot use branch (does not exist): b` with no metadata file written (Spark 4.1.2 writes to `b`: `spark2.out` `insb`, `delb`). The selector-door kernel call at `write_to_branch.rs:552` stays as belt-and-braces for the reader that keeps refs; no pin can reach it today. Retire with fork unit F-V1-REFS-1. |
| C-054 | `ALTER TABLE t WRITE ORDERED BY <terms>` lands transform terms as Spark does: bucket/truncate with either argument order and an int or long width literal (`4L`, `4l`), year/month/day/hour with their plural, `date` and `date_hour` names, `identity(col)`, DESC defaulting to nulls-last, duplicates kept, and `write.distribution-mode = range` (LOCALLY leaves it, DISTRIBUTED BY PARTITION sets `hash`). A later plain INSERT sorts by the transformed value and stamps the order id. | Spark probe plus Rust and facade pins; scoreboard replay. | PROVEN | `target/probe-u5-pr2b/spark.out`, `target/probe-u5-pr2b-r1fix/spark.out` (`wo_L`, `wo_tL`, `wo_Lup`). Pins: `alter_write_order_transform.rs::write_ordered_by_transforms_lands_the_order_spark_measured` (20 specs), `locally_and_distributed_transform_orders_keep_their_distribution`, `insert_after_a_bucket_order_sorts_by_the_bucket`, `test_write_ordered_by_transforms_lands_the_order_spark_measured`, `test_write_ordered_by_a_long_width_literal_lands_like_spark`. Replay `D-WRITE-ORDERED-TRANSFORM` EQUAL. |
| C-055 | The transform refusals RePark owns answer Spark's class and text and commit nothing: `Transform is not supported: <term>` (UnsupportedOperationException); `Term must be unbound`, `Unsupported width for transform: <term>`, `Cannot convert transform with more than one column reference: <term>`, `Cannot find width for transform: <term>` (IllegalArgumentException), with each literal rendered by value (`4S`/`4Y`/`4BD` → `4`, `4D`/`4F`/`4.0` → `4.0`, `3000000000L` → `3000000000`; round 4: an exponent literal renders as Java's double string, `1e2`/`1E2` → `100.0`, `1.5e1` → `15.0`, `-1e2` → `-100.0`, `1e10` → `1.0E10`, `1.5E-1` → `0.15`, `2e0` → `2.0`, and a bare-point decimal as its decimal string, `.5` → `0.5`, `-.5` → `-0.5`, `5.` → `5`, `1.50` → `1.50`). `bucket()`, `bucket(4,)`, `hours()`, `bucket(+4, id)`, `bucket((4), id)` and `bucket(4, id)(x)` raise AnalysisException with Spark's ANTLR text and `== SQL ==` block after RePark's `Error during planning: ` prefix (R-U5-PR2B-PLANNING-PREFIX). No token renders as `<other>`, and every token a message names is the token as typed: the tokenizer reads each other token back from its source span (round 3). A `0x…` token is an identifier, as Spark's lexer reads it; a column part outside `[A-Za-z_][A-Za-z0-9_]*` renders back-quoted (`bucket(0x4, id)` → `Cannot convert transform with more than one column reference: bucket(`0x4`, id)`, also `0X4`, `1abc`, `` `my col` ``, `` `a.b` ``; `bucket(0x4)` → `Cannot find width for transform: bucket(`0x4`)`); `X'4'` is a binary constant rendered `0x04` (`x'abc'` → `0x0ABC`, `X''` → `0x`), and a body that is not all ASCII hex digits raises AnalysisException `[INVALID_TYPED_LITERAL] The value of the typed literal "X" is invalid: '<body as typed>'. SQLSTATE: 42604` with the `== SQL ==` block, always naming `"X"` (`X'4g'`, `x'4g'`, `X'4G'`, `X' 4'`, `X'é'`, `X'zz'`; round 4 — head 5d367d44 rendered an invented `0x4G`); a string constant is unescaped as Spark's `unescapeSQLString` does before its embedded quote doubles (`truncate('a\'b', s)`, `'a''b'`, `"a'b"` → `truncate('a''b', s)`; round 4: `'a\\b'` → `'a\b'`, `'a\tb'`/`'a\nb'` → the control character, `"a""b"`/`"a\"b"` → `'a"b'`, `A`/`\U00000041`/`\101` → `A`, `\%`/`\_` keep the backslash, `\q`/`\f` → `q`/`f`, `\Z` → U+001A, `'a\\\'b'` → `'a\''b'`, `'\\'` → `'\'`). An empty order segment (`id ,`, `id DESC ,`, `id, s ,`, `bucket(4, id) ,`, `bucket(4, id) , ,`, `, id`, `id , , s`, `(id ,)`, `(, id)`, `(id , ,)`, `(bucket(4, id) ,)`, also after `LOCALLY` and `DISTRIBUTED BY PARTITION`) raises AnalysisException `no viable alternative at input '<token>'` naming the token that follows the gap (`','`, `')'` or `'<EOF>'`) and commits nothing — the metadata file count is unchanged (round 4; head 5d367d44 and every earlier head committed the order typed before the comma). | Spark probe plus Rust and facade pins. | PROVEN | `target/probe-u5-pr2b-r1fix/spark.out`, critic `/tmp/xrv-probe/mal.spark.out`. Pins: `alter_write_order_transform.rs::write_ordered_by_transform_refusals_match_spark_and_commit_nothing`, `typed_width_literals_and_parse_shapes_answer_spark`, `test_write_ordered_by_transform_refusals_match_spark` (`short-width`, `empty-arguments`). Round 3 (critic r2 V-003, V-004): head 362b73d2 rendered `0x4` through sqlparser's Display as `X'4'` inside a fabricated ANTLR message, and `'a\'b'` as `'a'b'`; Spark re-measured 2026-09-25 (`scratchpad/probe/spark.out` `wo_hex` … `wo_hexdesc`, `spark2.out` `wo_xAB` … `wo_dottedq`). Pins: `hex_quoted_and_string_tokens_render_as_spark_does`, `test_write_ordered_by_transform_refusals_match_spark` (`hex-token-is-a-quoted-reference`, `string-constant-doubles-its-quote`). The fork's bind text is residue R-U5-PR2B-BIND-TEXT. Round 4 (critic r3 V-001, V-002, V-003, V-006; Spark 4.1.2 + Iceberg 1.11.0 re-measured 2026-09-25: `scratchpad/probe/spark2.out` `wo_bi`…`wo_br`, `spark5.out` `wo_da`…`wo_df`, `spark6.out` `wo_hx1`…`wo_nm11`; RePark on the fixed wheel `repark6.out`, `repark5b.out` equal after the R-U5-PR2B-PLANNING-PREFIX prefix). Red first: `bucket(1e2, id)` rendered `1e2`, `truncate('a\\b', s)` rendered `'a\\b'`, and `WRITE ORDERED BY id ,` committed order 1. Pins: `hex_quoted_and_string_tokens_render_as_spark_does` (the escape rows and `INVALID_HEX_ROWS`), `typed_width_literals_and_parse_shapes_answer_spark` (the exponent and decimal rows), `alter_write_order.rs::write_order_malformed_shapes_refuse` (fourteen comma shapes with full text and the metadata-file-count assertion). The tokenizer behind the door lets a backslash escape inside a string, so a `\'` no longer sends the statement through the literal canonicalizer. |
| C-056 | `ALTER TABLE t CREATE BRANCH b` with no `AS OF` on a snapshot-less table commits an empty append (Spark's summary counters, sequence number 1 on v2) and points `b` at it; `main` and the snapshot-log stay absent. IF NOT EXISTS, OR REPLACE of a new branch, retention and `CREATE BRANCH main` follow Spark. Tags, a replace of an existing branch, bare REPLACE and a duplicate raise Spark's IllegalArgumentException texts. | Spark probe plus Rust and facade pins; scoreboard replay. | PROVEN | `target/probe-u5-pr2b/spark.out`, `spark2.out`. Pins: `ref_branch_on_empty.rs`, `test_create_branch_on_an_empty_table_commits_an_empty_append_like_spark`, `test_refs_on_an_empty_table_refuse_like_spark`. Replay `D-REF-BRANCH-ON-EMPTY` EQUAL. Retention is residue R-U5-PR2B-RETENTION-SECOND-COMMIT. |
| C-057 | On a format v1 table, merge-on-read DELETE, UPDATE and MERGE raise IllegalArgumentException `Deletes are supported in V2 and above`, as Spark does. On the Spark door, `ALTER TABLE <vN> SET TBLPROPERTIES ('format-version'='<M>')` with an integer M below N raises `Unsupported table change: Cannot downgrade vN table to vM`, surfacing as PySparkException `datafusion engine error: Execution error: …` (the PR2a precedent for Spark's SparkException). | Spark probe plus Rust and facade pins. | PROVEN | `target/probe-u5-pr2b-r1fix/spark.out` (`mor_del`, `mor_upd`, `mor_merge`, `down`). Pins: `create_format_version_one.rs::merge_on_read_row_level_writes_on_a_v1_table_refuse_like_spark`, `write/merge/tests/streaming_scan.rs::mor_on_v1_table_is_rejected_before_any_write`, `v3_upgrade.rs::alter_downgrade_and_unsupported_versions_refuse_naming_both_versions`, `test_v3_upgrade.py::test_alter_downgrade_and_unsupported_versions_refuse`. |
| C-058 | A hive-style typed column in a column-def `CREATE TABLE … PARTITIONED BY (<name> <type> [NOT NULL] [COMMENT '<doc>'], …)` is appended to the table schema after the declared columns, in clause order, with the next field ids, and gets an identity partition field; `NOT NULL` makes it required and `COMMENT` lands the field doc. A table with no column list takes only the typed columns. `CREATE OR REPLACE` / `REPLACE TABLE` re-key it like a declared column (Spark's id 4 and spec field 1001). A typed column beside an untyped column or a transform raises ParseException `Operation not allowed: PARTITION BY: Cannot mix partition expressions and partition columns:\nExpressions: <e, …>\nColumns: <name type, …>.`; a typed name that repeats a declared or earlier typed column (case-insensitive) raises AnalysisException `[COLUMN_ALREADY_EXISTS] The column `<lower-cased name>` already exists. …SQLSTATE: 42711`; neither creates a table. The old `PARTITIONED BY typed column … is not supported` refusal is gone. `PARTITIONED BY (cat)` and `PARTITIONED BY (days(ts))` are unchanged. | Spark probe plus Rust and facade pins, cell replay. | PROVEN | `target/probe-u5-pr3/spark.out` (`c_*`), `spark2.out`; replay `target/probe-u5-pr3/replay/after.json` D-X-PARTITIONED-COLDEF EQUAL. Pins: `create_typed_partition.rs` (four tests), `test_ice_ddl_alter_2.py::test_typed_partition_columns_become_identity_columns_like_spark`, `…::test_typed_partition_column_refusals_match_spark`. |

### PR2b residues (dated 2026-09-25)

- R-U5-PR2B-V1-REFS: the fork writes format v1 metadata without `refs` (Java writes them), so `D-CREATE-V1` shows `md.refs` `[]` against Spark's `[[main, branch, S0]]`. C-053 refuses every non-main ref write until fork unit F-V1-REFS-1 lands. A RePark commit to a Java-written v1 table that carries branches or tags drops them (fork-level, pre-existing).
- R-U5-PR2B-FORK-V1-SERIAL: the fork omits an empty `snapshots` array, writes `sequence-number 0` on legacy snapshots after a v1 → v2 upgrade (Java omits the key; Spark `UPSNAP ["ABSENT"]`), and answers `CREATE OR REPLACE` v2 → v1 with `DataInvalid => Cannot downgrade FormatVersion from v2 to v1` where Spark says `Cannot downgrade v2 table to v1`.
- R-U5-PR2B-FORMAT-VERSION-VALUES: Spark writes metadata v0, v-1 and v4 for `'0'`, `'-1'` and `'4'`; RePark refuses them as not implemented. PySpark's NumberFormatException leaf is not defined, so `'abc'` raises its parent IllegalArgumentException. `' 1 '` is trimmed and accepted, where Spark raises `For input string: " 1 "`.
- R-U5-PR2B-BIND-TEXT: the bind refusal is the fork's. RePark raises PySparkException `DataInvalid => Cannot bind: day cannot transform long values from 'id'` (pinned as a residue by `test_write_ordered_by_bind_refusal_keeps_the_fork_text_residue`); Spark raises ValidationException `Cannot bind: day cannot transform long values from 'id'`. For a struct source the fork prints `struct<intstring>`, where Spark prints `struct<5: a: optional int, 6: b: optional string>`.
- R-U5-PR2B-UNKNOWN-FIELD: an unknown column in a term (also `id.x`, `sc.ns.wo.id`) keeps `DataInvalid => Cannot find field nope in table schema`; Spark says `Cannot find field 'nope' in struct: struct<…>`.
- R-U5-PR2B-OTHER-MALFORMED: `bucket(`, `bucket(4, id`, `bucket(4, id))`, `… DESC DESC`, `… NULLS` and `year(ts) LOCALLY` keep RePark's analysis texts; Spark gives the ANTLR texts in critic `/tmp/xrv-probe/mal.spark.out`. `bucket(4, 0xid)` (round 3): sqlparser splits `0x` from `id`, so RePark answers `ALTER TABLE WRITE ORDERED BY transform `bucket ( 4 , 0x id )` takes column names and constants only (transform `bucket`)`, where Spark reads one identifier and raises ValidationException `Cannot find field '0xid' in struct: …`. Round 4 (critic r3 V-005, dated 2026-09-25; `scratchpad/probe/spark.out` `wo_ag`, `wo_u`, `wo_ak`, `spark2.out` `wo_bc`…`wo_be`, `wo_ml2`, `spark3.out` `wo_ca`…`wo_cj`, `spark6.out` `wo_hx6`, each against the `repark*.out` twin): `N'…'` typed literals (`bucket(N'4', id)`, `truncate('ä''b', N'x')`) answer RePark's ANTLR-shaped `no viable alternative at input 'N'4''` where Spark raises `[UNSUPPORTED_TYPED_LITERAL] Literals of the type "N" are not supported. Supported types are "DATE", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP", "INTERVAL", "X", "TIME". SQLSTATE: 0A000`; `bucket(-0x4, id)`, `bucket(0xé, id)` and ``bucket(`cölumn` + 4, id)`` answer `ALTER TABLE WRITE ORDERED BY transform `…` takes column names and constants only (transform `bucket`)` where Spark raises `no viable alternative at input '-0x4'`, `extraneous input 'é'` and `mismatched input '+' expecting {',', ')'}`; a trailing token after a valid term (`bucket(4, id) +`, `… x`, `… 0x4`, `… 'x'`, `… X'4'`, `… N'x'`) answers `trailing tokens after WRITE ORDERED BY column …` where Spark raises `extraneous input '+' expecting <EOF>` / `mismatched input …`; and a parse error later in the list is masked by an earlier term's refusal (`bucket(4, id), bucket(0x4, id) +` and the multi-line `truncate('ä\nb', s), bucket(`cölumn` + 4, id)` answer IllegalArgumentException, `bucket(X'4g', id), bucket(4,)` answers INVALID_TYPED_LITERAL, where Spark's ANTLR pass raises `mismatched input '+'` / `no viable alternative at input ')'` first). All refuse, nothing commits, every named token is as typed.
- R-U5-PR2B-PLANNING-PREFIX (dated 2026-09-25, pre-existing shape): the ANTLR-shaped refusals of C-055 and the REF-1 missing-branch refusal are `DataFusionError::Plan`, so RePark raises AnalysisException `Error during planning: <Spark's text>` where Spark raises ParseException (`\nno viable alternative at input ')'\n== SQL ==\n…`) or ValidationException (`Cannot use branch (does not exist): b`). The text after the prefix is Spark's. Pinned as full text by `typed_width_literals_and_parse_shapes_answer_spark`, `writes_into_a_missing_branch_on_a_v1_table_answer_the_missing_branch_text` and `test_write_ordered_by_transform_refusals_match_spark[empty-arguments]`.
- R-U5-PR2B-WRITETO-BRANCH-OPTION (dated 2026-09-25, pre-existing): `df.writeTo(t).option('branch', 'b').append()` is refused by the Python facade (`writer_readwriter.py`, UnsupportedOperationException `writing to an Iceberg branch is not supported — repark write path is current-snapshot only (I1 / R-TIME-TRAVEL)`) before any Rust path, so the C-053 kernel is not what answers it. Spark 4.1.2 ignores the option and appends to `main` (critic r2 V-002, `/tmp/xr2-probe/spark.out` `dfbranch`). U7 PR2 slice 1 (repark#835) removes the facade refusal; the write then takes `commit_target::maybe_to_branch`, which the kernel pin covers.
- R-U5-PR2B-IDENTITY-ONLY-DML: after a RePark-set transform order, UPDATE, DELETE, MERGE INTO and INSERT OVERWRITE raise `This feature is not implemented: sorting by the table's default sort order uses transform `bucket[4]` on source id 1, only identity sort fields are supported`; Spark runs all four and stamps `sort_order_id` 1 (critic `/tmp/xrv-probe/wp.py`; `target/probe-u5-pr2b-r1fix/spark.out` `wp_del`). INSERT INTO, `writeTo().append()` and `rewrite_data_files` succeed. Pinned by `delete_after_a_repark_bucket_order_is_the_identity_only_residue`.
- R-U5-PR2B-RETENTION-SECOND-COMMIT: `WITH SNAPSHOT RETENTION`/`RETAIN` on an empty-table branch is a second commit. The fork's `ManageSnapshots` requirement for a ref that an earlier action of the same transaction set is checked against the base table; the fork change is to drop that requirement. Observable: one extra metadata-log entry per statement that carries retention (RePark 2 vs Spark 1 after one statement, 6 vs 3 after three; snapshots equal; `target/probe-u5-pr2b-r1fix/repark_rt.py`, `spark_rt.out`). The two commits are non-atomic. The empty append carries `engine.operation-id` where Spark writes `app-id`/`app-name`/`engine-name`/`engine-version`/`iceberg-version`. No cell compares either. Beside it (critic r3 V-007, dated 2026-09-25; `scratchpad/probe/spark.out` vs `repark.out`, `SO` lines `z`, `aa`): re-setting an identical write order (`bucket(4, 0x4)` after ``bucket(4, `0x4`)`` against a real `0x4` column) writes a new metadata file in RePark (nmeta 6 → 7, order id reused) where Spark writes nothing (nmeta stays 6), so every later count in that run is offset by one. `write_order_identical_order_reuses_its_id` pins the id reuse, not the absence of a commit.
- R-U5-PR2B-ANSI-EMPTY-BRANCH: the ANSI door keeps `needs AS OF VERSION <snapshot-id>` for CREATE BRANCH on an empty table.
- R-U5-PR2B-ALTER-OTHER-TOKEN: `alter.rs` keeps its own tokenizer, whose unknown tokens still render as `<other>` on the non-write-order ALTER routes.

### PR3 residues (dated 2026-09-25)

- R-U5-PR3-MIX-RENDER: the mix refusal renders each typed column as its SQL spelling, lower-cased (`INTEGER` → `integer`, `STRUCT<a: INT>` → `struct<a: int>`), where Spark prints its type name (`int`, `struct<a:int>`); the pinned shapes (`string`, `int`, `date`, `decimal(10,2)`, `array<int>`) match. The facade shows the parse error as `SQL error: ParserError("…")` with escaped newlines and no `== SQL` context, like the other `Operation not allowed` pins.
- R-U5-PR3-NON-PRIMITIVE: a STRUCT or ARRAY typed partition column raises PySparkException `DataInvalid => Cannot partition by non-primitive source field: 'struct<int>'.` (the fork's text); Spark raises Py4JJavaError ValidationException `Cannot partition by non-primitive source field: struct<2: a: optional int>`. Neither creates a table.
- R-U5-PR3-CTAS-TYPED (pre-existing): CTAS with a typed partition column keeps RePark's AnalysisException `Partition column types may not be specified in Create Table As Select (CTAS): partition column `p` carries a data type. …`; Spark raises ParseException `Operation not allowed: Partition column types may not be specified in Create Table As Select (CTAS).`
- R-U5-PR3-RENAME-TABLE (HALT, 2026-09-25): D-RENAME-TABLE is not changed. Spark 4.1.2 reads the whole `RENAME TO` target as `namespace…name` inside the source catalog on every Iceberg catalog, not only on hadoop (`target/probe-u5-pr3/spark.out` `sc_*`, `hc_*`); the recorded cell runs on the InMemoryCatalog `sc`, whose RePark twin is the default memory catalog the brief keeps unchanged. Ruling asked in the hand-back.
