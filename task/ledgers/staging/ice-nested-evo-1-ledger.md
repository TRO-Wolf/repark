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
| C-027 | Nested type targets use the top-level promotion validation and a map key keeps Spark's refusal class, condition, SQLSTATE, and message. | Spark probe and exact Rust/facade refusal pins. | PROVEN | Spark rejects `m.key` with `NOT_SUPPORTED_CHANGE_COLUMN`, SQLSTATE `0A000`, and the measured full message. Rust and facade pins compare the exact RePark error. |
| C-028 | The three parser additions remain narrow: top-level TYPE, non-primitive top-level TYPE, and UNSET without IF EXISTS preserve their prior routes. | Direct Rust parser pins and facade near-miss pins. | PROVEN | Parser pins leave top-level TYPE and nested column-move forms on their prior paths. Mutation runs red for the nested commit, UNSET rewrite, and namespace property merge. |

### Out-of-scope observation

- Spark accepts `ALTER NAMESPACE ... UNSET DBPROPERTIES (...)` and `UNSET PROPERTIES (...)`. This PR does not add the distinct namespace UNSET operation.
