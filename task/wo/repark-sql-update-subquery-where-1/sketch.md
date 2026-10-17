# Sketch REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (ruling 106): `repark.sql()` against the ANSI door, measured; routing and the stopgap

**Date:** 2026-10-16. **Author:** Claude Opus 5.5 (opus-worker, design round 1). **Status:** sketch,
documents only; it halts before code for the questions in §11. **Main at writing:** `0f074aff`.
**Card:** [REPARK-SQL-UPDATE-SUBQUERY-WHERE-1](../../roadmap/mid-term/repark-sql-update-subquery-where-1-card-2026-10-15.md).
**Registry row:** REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 in [the parity registry](../../../docs/spark-sql-iceberg-parity.md).
**Rulings:** 106 (a), 113, 116 (in part). **Retires:** when the 106 build merges, or when the owner
rejects this sketch; it is then frozen with the unit's records.

## 1. Summary

- Through `repark.sql()`, an `UPDATE` or `DELETE` whose `WHERE` holds a subquery writes wrong rows
  and reports success. Most `UPDATE` forms write every row; most `DELETE` forms empty the table.
  15 of the 17 card cells are wrong; the other 2 (a correlated scalar subquery) refuse with
  DataFusion's planner text. Spark 4.1.2 writes only the matching rows in all 17.
- The card names `UPDATE` only. `DELETE` is the worse half: 6 of its 8 forms delete every row.
- The ANSI door refuses 12 of the 17 with the G3-E8 text. It answers the other 5 (three-part
  `IN`, `EXISTS`, `NOT IN` forms) with Spark's rows, through the identity path. The Spark SQL door
  answers the same 5 and the two unqualified card spellings, and refuses the other 10.
- Routing `repark.sql()` through `AnsiDialect` needs a `repark-python → repark-sql` edge, which
  ruling 113 makes a halt. Routing fixes 23 wrong answers, but it makes 11 measured statements
  worse, in 5 classes, and it turns five pinned expectations on main red.
- Recommendation: 106 builds the stopgap without a third guard copy. The ANSI door's refusal moves
  to one home in `repark-iceberg`, which `repark-core` reaches. `DataFusionDialect` (the
  `repark.sql()` door) and the ANSI router both call it. No crate edge, no manifest change.
  Routing stays on the card behind the owner's rulings (§11).

## 2. Measurements on main (step 0)

### 2.1 Setup

- **Module.** Built from the clean tree at `0f074aff` inside the build lock, with `maturin develop`
  (the `make develop` profile: dev, features `extension-module` and `allocator-mimalloc`).
  `git status --short` printed nothing before the build. `_native.abi3.so` sha256
  `976605d7db7b09f9b138cd5ee67e6729e189c86419eb4bca9e7f4dd2c1cc3cc9`.
- **RePark doors.** Each cell gets a fresh memory catalog `ice`, the namespace `ice.ns` with a
  location, and two tables made by `CREATE TABLE … AS SELECT`:
  - `t (id INT, v INT)`: (1, 10), (2, 20), (3, 30), (4, 40);
  - `s (id INT, w INT)`: (2, 20), (3, 99).
- **`repark.sql()`.** The public callable over `PyReparkSession.native()` with the catalog
  registered, the pattern of `python/repark/tests/test_update_set_subquery_walk_1.py`.
- **Spark SQL door.** The Python facade, `ReparkSession.builder…getOrCreate()`.
- **ANSI door.** A Rust session built with `AnsiDialect`, run from a throw-away test file in
  `crates/repark-sql/tests/`. The file was removed after each run and is not committed.
- **Oracle.** PySpark 4.1.2 with `iceberg-spark-runtime-4.1_2.13` 1.11.0, `local[1]`, zulu-17,
  UTC, a Hadoop catalog `ice` over a private temporary warehouse per run. Each cell drops and
  re-seeds both tables. The statement text is the same as on the RePark doors.

### 2.2 The card cells

`UPDATE` cells show `v` for ids 1 to 4 after the statement; `DELETE` cells show the ids that
remain. **G3-E8** is a refusal with the G3-E8 text, rows unchanged. **DF** is a refusal by
DataFusion's planner ("Correlated scalar subquery must be aggregated to return at most one row"),
rows unchanged. A bold cell is a wrong answer that reports success.

| # | Statement (`UPDATE ice.ns.t SET v = 5 WHERE …` / `DELETE FROM ice.ns.t WHERE …`) | Spark 4.1.2 | `repark.sql()` | ANSI door | Spark SQL door |
|---|---|---|---|---|---|
| U1 | `id IN (SELECT id FROM ice.ns.s)` | 10 5 5 40 | **5 5 5 5** | 10 5 5 40 | 10 5 5 40 |
| U2 | `id = (SELECT max(id) FROM ice.ns.s)` | 10 20 5 40 | **5 5 5 5** | G3-E8 | G3-E8 |
| U3 | `EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)` | 10 5 5 40 | **5 5 5 5** | G3-E8 | G3-E8 |
| U4 | `v = (SELECT s.w FROM ice.ns.s WHERE s.id = t.id)` | 10 5 30 40 | DF | G3-E8 | G3-E8 |
| U5 | `id NOT IN (SELECT id FROM ice.ns.s)` | 5 20 30 5 | **5 5 5 5** | G3-E8 | G3-E8 |
| U6 | `EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)` | 10 20 30 40 | **5 5 5 5** | G3-E8 | G3-E8 |
| U7 | `id IN (SELECT id FROM ice.ns.s) AND v > 25` | 10 20 5 40 | **10 20 5 5** | G3-E8 | G3-E8 |
| D1 | `id IN (SELECT id FROM ice.ns.s)` | 1 4 | **none** | 1 4 | 1 4 |
| D2 | `id = (SELECT max(id) FROM ice.ns.s)` | 1 2 4 | **none** | G3-E8 | G3-E8 |
| D3 | `EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)` | 1 4 | **none** | 1 4 | 1 4 |
| D4 | `v = (SELECT s.w FROM ice.ns.s WHERE s.id = t.id)` | 1 3 4 | DF | G3-E8 | G3-E8 |
| D5 | `id NOT IN (SELECT id FROM ice.ns.s)` | 2 3 | **none** | 2 3 | 2 3 |
| D6 | `EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)` | 1 2 3 4 | **none** | 1 2 3 4 | 1 2 3 4 |
| D7 | `id IN (SELECT id FROM ice.ns.s) AND v > 25` | 1 2 4 | **1 2** | G3-E8 | G3-E8 |
| C1 | the card's spelling, `UPDATE t SET v = 5 WHERE id IN (SELECT id FROM s)` | 10 5 5 40 | **5 5 5 5** | G3-E8 | 10 5 5 40 |
| C2 | `DELETE FROM t WHERE id IN (SELECT id FROM s)` | 1 4 | **none** | G3-E8 | 1 4 |
| C3 | `UPDATE t SET v = 5 WHERE id = (SELECT max(id) FROM s)` | 10 20 5 40 | **5 5 5 5** | G3-E8 | G3-E8 |

- C1 to C3 run after `SET datafusion.catalog.default_catalog = 'ice'` and
  `SET datafusion.catalog.default_schema = 'ns'` on the RePark doors, and after `USE ice.ns` on
  Spark and on the Spark SQL door.
- Spark writes one new snapshot in every cell (1 → 2), U6 and D6 included.
- `repark.sql()` answers every wrong cell with a `count` row (4 for U1).
- A Rust session with the bare `DataFusionDialect` answers all 17 as `repark.sql()` does. A Rust
  Spark session without the facade answers as the facade, except C1 and C2, which it refuses.
- Two more shapes are right on main only by coincidence, because the predicate holds for every
  row: `DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.t)` (Spark and main: no row left)
  and `UPDATE ice.ns.t SET v = 5 WHERE EXISTS (SELECT 1 FROM ice.ns.s)` (Spark and main: 5 5 5 5).
- The defect is not Iceberg-only. On an in-memory table made by
  `CREATE TABLE mem3 AS SELECT * FROM (VALUES (1), (2), (3)) AS x(a)`,
  `DELETE FROM mem3 WHERE a IN (SELECT 1)` deletes all three rows and
  `UPDATE mem3 SET a = 5 WHERE a IN (SELECT 1)` sets all three. `UPDATE … WHERE id = ANY (SELECT …)`
  also writes every row.

### 2.3 Where the ANSI door refuses, and where it answers instead

- **The refusal.** `refuse_dml_subquery_predicate` in `crates/repark-sql/src/guards.rs`, lines
  485–514. The guard-6 block is lines 447–569: `DmlSubqueryVerb` 451–483, `delete_target` and
  `update_target` 516–527, `expression_contains_subquery` 537–549, `dml_subquery_refusal_message`
  551–569. The guard returns `Ok` for the allow-listed forms. It refuses any other `DELETE` or
  `UPDATE` whose `WHERE` holds a `Query` node.
- **Its callers.** `crates/repark-sql/src/router.rs` line 198, in `execute_update_routed`, before
  the merge-on-read and update-cast valves; and line 266, in `execute_identity_or_delegate`, after
  the identity attempts and before `delegate`.
- **The answers.** `execute_identity_or_delegate` (`router.rs` 235–269) commits the allow-listed
  forms through `commit_identity_dml` (290–301). The allow-lists are
  `repark_iceberg::write::predicate_dml::try_allowed_delete_in` (`predicate_dml.rs` line 140:
  `DELETE` with `col [NOT] IN (SELECT col FROM src …)` or `[NOT] EXISTS (SELECT … FROM src …)` that
  names only the source and the target) and `predicate_dml::plain::try_allowed_update_in`
  (`plain.rs` line 185, through `is_allowed_positive_uncorrelated_in` at `predicate_dml.rs` line
  643: `UPDATE` with a positive uncorrelated `col IN (SELECT col FROM src …)` and no subquery in
  `SET`). Both need a three-part target, so C1 and C2 refuse on the ANSI door.
- **The Spark SQL door's copy.** `crates/repark-spark/src/normalize.rs` lines 311–436
  (`refuse_dml_subquery_predicate` 349, `refuse_dml_subquery_predicate_in_statement` 368,
  `dml_subquery_refusal_message` 421), with the same text. A third copy of the subquery probe is
  `expression_contains_subquery` at `predicate_dml.rs` line 703; like the two door probes, it
  stops at any `Query` node.
- **The card's sentence** "The Spark SQL door and the Rust ANSI door refuse these statements" holds
  on both doors for U2–U7, D2, D4, D7 and C3, and on the ANSI door alone for C1 and C2. Both doors
  answer U1, D1, D3, D5 and D6 with Spark's rows.

### 2.4 The orchestrator's facts, checked

1. **Confirmed.** `PyReparkSession::native` (`crates/repark-python/src/session.rs` from line 150)
   sets no dialect, so `build()` installs `DataFusionDialect` (`crates/repark-core/src/session.rs`
   line 318). `DataFusionDialect::execute` (`crates/repark-core/src/dialect.rs` 130–139) is
   `PreExecute::run` (`pre_execute.rs` 59–63): plan, `guard` (42–47), execute. The `sql`,
   `sql_built` and write-option entries all reach `dialect.execute_with_write_options`.
2. **Confirmed.** `AnsiDialect` (`crates/repark-sql/src/dialect.rs` 11–55) executes through
   `crate::router::execute` (`router.rs` line 23).
3. **Confirmed.** `repark-python` takes `repark-core` (feature `postgres`), `repark-functions`,
   `repark-ta` (feature `datafusion`), `repark-ml` and `repark-spark` as normal dependencies, and
   `repark-common` as a dev dependency. `repark-sql` is a deliberate non-edge in the policy row
   `("repark-python", "repark-spark")` of `scripts/check_crate_dag.py` ("no ANSI surface from
   Python") and in [crates/map.md](../../../crates/map.md).
4. **Confirmed.** `NativeDoorTypes` (`crates/repark-python/src/session_runtime.rs` 395–413) returns
   `repark_functions::spark_type_planner::ansi_type_planner()` and
   `analyzer_rules_with_timestamp_unit_widen`, the two calls `AnsiDialect` makes. The Q6 cell
   answers the same on `repark.sql()` and on the ANSI door (§3).
5. **Confirmed in part.** U-5 is not on main. Its branch head `d38ac00b` (read only) defines
   `LocalFsGuardedDialect::wrapping(inner)` in `crates/repark-core/src/local_fs_policy.rs`. The
   wrapper refuses local-file statements, then calls `inner.execute`, and it forwards
   `execute_with_write_options`, `on_session_built`, `type_planner` and
   `configure_analyzer_rules`. `ReparkSessionBuilder::with_local_fs_guard()` wraps the dialect the
   builder holds. The pull request number was not checked: this lane has no `gh`.

## 3. The divergence census

### 3.1 Method

- **Statements.** 136: the 17 card cells and 119 census statements. The coincidence shapes of
  §2.2 were measured apart, on `repark.sql()` and Spark.
- **Sources.** The three named divergences (PA-02 local files, Q94 `TIMESTAMP` typing, this card);
  every module-level `repark.sql()` call in `python/repark/tests` (28 files); one or more
  statements for each arm of the ANSI router (`router.rs` 23–191 and the pre-parse stage). Another
  19 test files call `.sql` on a facade session that they name `repark`; that is the Spark door,
  and it is out of the census.
- **Doors.** `repark.sql()` (Python, the module of §2.1); the ANSI door (Rust); a Rust replica of
  today's native door; a Rust replica of the routed door. The native replica is
  `DataFusionDialect`, `NativeDoorTypes`, the three native-door registrations, the binding's shared
  analyzer rules, and the binding's declared-function refusal and map-cast rewrite. The routed
  replica is `AnsiDialect`, an extension that attaches only the `repark.sql` conf, the binding's
  shared analyzer rules, and the same binding pre-pass. The native replica agrees with
  `repark.sql()` on 132 of 136 statements. The other 4 differ only in display (`EXPLAIN ANALYZE`
  timings, list and map formatting).
- **What the counts compare.** The native replica against the routed replica, through one render,
  so each difference is the dialect alone. A routed variant that keeps `NativeDoorTypes` unchanged
  (the widen rule then sits twice before `type_coercion`) and keeps `register_native_door_functions`
  answers all 136 as the routed replica does. The bare ANSI door differs from the routed replica
  only on `CAST(true AS DECIMAL(…))`, which the binding's shared analyzer rule answers.

### 3.2 Counts

| Class | Statements |
|---|---|
| Identical | 46 |
| List 1: wrong → Spark's answer | 5 |
| List 1: wrong → refusal | 14 |
| List 1: PA-02, a local file written outside every warehouse → refused | 4 |
| List 2: never worse (main answers right, routing makes it worse) | 11 |
| Refusal → answer (new surface on `repark.sql()`) | 14 |
| Both refuse, with a different text or exception class | 17 |
| Both refuse with the same text (a readback type name differs) | 6 |
| Same rows; a string column is `Utf8View` on main and `Utf8` routed (the binding exports both as `string`) | 6 |
| Result shape only: DDL answers one zero-column row (main: none); identity-path DML answers no `count` row | 13 |
| **Total** | **136** |

### 3.3 List 1: routing turns a wrong answer into a refusal or a right answer (23)

| Class | Statements | `repark.sql()` on main | Routed |
|---|---|---|---|
| G3-E8, allow-listed forms | U1, D1, D3, D5, D6 | every row written, or the table emptied | Spark's rows (identity path) |
| G3-E8, other forms | U2, U3, U5, U6, U7, D2, D7, C1, C2, C3, `UPDATE … WHERE id = ANY (SELECT …)` | wrong rows | G3-E8 refusal, rows kept |
| G3-E8 on an in-memory table | `DELETE FROM mem3 WHERE a IN (SELECT 1)`, `UPDATE mem3 SET a = 5 WHERE a IN (SELECT 1)` | every row deleted or updated | G3-E8 refusal |
| G15 in DDL | `CREATE TABLE ice.ns.cc (s VARCHAR COLLATE "de_DE")` | the table is made; the collation is dropped without a word | G15 refusal |
| PA-02 | `COPY … TO '<outside>/…'`, `CREATE EXTERNAL TABLE … LOCATION '<outside>/…'`, and the `COPY` after either `SET repark.sql.…` spelling | the file is written outside every warehouse | SEC-02 refusal; nothing written |

U4 and D4 are refusals on both sides; they are in the text-change class below.

### 3.4 List 2: the never-worse list (11 statements, 5 classes)

The owner sees this list before any routing build.

| # | Class | Statements | `repark.sql()` on main | Routed | Pin on main that goes red |
|---|---|---|---|---|---|
| N1 | In-memory tables by bare name | `CREATE TABLE mem2 (a INT)`; `CREATE TABLE IF NOT EXISTS mem_t AS SELECT value AS v, value % 7 AS k FROM generate_series(1, 5)`; `DELETE` and `UPDATE` with a plain predicate on such a table | made and answered | refused: "CREATE TABLE AS SELECT target `mem_t` is not a qualified Iceberg table name …" | `test_offset_nested_sort_1.py::test_native_sql_door_answers_spark_rows` |
| N2 | `TIMESTAMP` in DDL | `CREATE TABLE ice.ns.ts (t TIMESTAMP)`, `CREATE TABLE ice.ns.tstz (t TIMESTAMPTZ)` | a microsecond column | refused: "column `t` is TIMESTAMP with nanosecond precision (9) …" | none |
| N3 | The local-file opt-in | `COPY … TO '<outside>/…'` with `repark.sql.allowLocalFilesystemDDL = true` on the builder | written | refused (the gate cannot read the opt-in, §12) | none; U-5 gives `repark.sql()` no opt-in |
| N4 | Internal kernels by name | `SELECT __repark_cast_timestamp_ns__('2026-01-02 03:04:05')`, `__repark_cast_timestamp_ntz__(…)`, `__repark_store_int32__(1)` | `UNRESOLVED_ROUTINE` | the kernels resolve | the `r013\|ansi_kernel_not_a_routine` cell of `test_ice_tsns_type_planner_1.py::test_refusals_microsecond_pins_and_registry_rows` |
| N7 | `DESCRIBE` | `DESCRIBE ice.ns.t` | `data_type` `Utf8View` for a string column | `Utf8` | none |

N5 and N6 are the result-shape and text classes of §3.5; they are changes, not regressions, but
three of the text changes break pins on main:

- `test_ice_procs_route_1.py`, the `check_native_refused` helper (four tests): it expects
  "Unsupported SQL statement"; the routed `CALL` text is "CALL `…` is not supported: maintenance in
  this engine runs as a CALLABLE OPERATION …".
- `test_dml_c_truncate.py::test_ansi_callable_truncate_no_longer_uses_the_refuse_substitute`: it
  expects "table 'ice.sales.does_not_exist' not found"; the routed text is
  "[TABLE_OR_VIEW_NOT_FOUND] The table or view … cannot be found …".
- `test_ice_rtas_byname_1.py::test_thin_native_door_parse_errors_loudly`: it expects a
  `ParseException`; the ANSI door's wrong-door hint wraps the parse error as a planning error, so
  the binding raises `AnalysisException` (the text still names `BY`).

### 3.5 Other changes routing brings

- **Refusal → answer (14).** `ALTER TABLE … ADD COLUMN`, `RENAME COLUMN`, `DROP COLUMN`,
  `RENAME TO`, `ALTER COLUMN … FIRST`, a nested `ADD COLUMN s.b`, `CREATE BRANCH`, `MERGE INTO`,
  `FOR TIMESTAMP AS OF`, `CREATE TABLE … WITH (partitioning = …)`, `… PARTITIONED BY (p)`,
  `INSERT OVERWRITE … PARTITION (…)`, `INSERT … VALUES (…, DEFAULT)`, `TRUNCATE TABLE`.
- **Text or class changes (17).** A multi-statement script, `ALTER TABLE … EXECUTE`,
  `SET PROPERTIES`, `SET TBLPROPERTIES`, `INSERT … BY NAME`, `CREATE TABLE … USING iceberg`,
  `LATERAL VIEW`, `COLLATE` in a `SELECT`, a whole-table `INSERT OVERWRITE`, a short `VALUES`
  arity, an `INSERT` into `t.branch_b1`, `CALL`, `TRUNCATE` of a missing table, `DELETE` from a
  missing table (the ANSI text is a raw `TableNotFound => No such table: TableIdent { … }`),
  `UPDATE … SET id = 'abc'`, U4, D4.
- **Result shape (13).** Routed DDL answers one zero-column row where main answers none, and the
  identity-path `DELETE` and `UPDATE` answer no `count` row where main answers one. The delegated
  DML (`IN` lists, `INSERT`) keeps its `count`.
- **Identical (46).** Among them the Q6 cell (Q94: `TIMESTAMP` and the typed literal answer
  `timestamp[us]`, `TIMESTAMPTZ` answers `timestamp[us, tz=UTC]`, value `…05.123456`), the
  `SELECT` pins of the 28 test files (integer overflow, `log`, `log1p`, `try_*` unresolved, decimal
  nullability, map casts, `range`), `SET` and `RESET`, `EXPLAIN`, `SHOW TABLES`, views, prepared
  statements and metadata tables.

### 3.6 The stopgap's own never-worse list

The recommended stopgap (§6) changes only an `UPDATE` or `DELETE` whose `WHERE` holds a subquery.
In that class, main is right only by coincidence: the two shapes at the end of §2.2 become
refusals. U4 and D4 change refusal text, from DataFusion's planner text to the G3-E8 text.
Nothing else changes; §9 re-runs the 46 identical statements as the control.

## 4. Options

### (A) A `repark-python → repark-sql` dependency

- **Files.** `crates/repark-python/Cargo.toml` (one dependency line; owner only); `Cargo.lock`
  (one entry in `repark-python`'s list; owner only); `scripts/check_crate_dag.py` (a new
  `("repark-python", "repark-sql")` normal row, and the non-edge sentence of the
  `("repark-python", "repark-spark")` row); `crates/repark-python/src/session.rs` (one dialect
  line, and the `register_native_door_functions` call goes, because `AnsiDialect`'s
  `on_session_built` installs a superset); prose in `crates/map.md` ("five edges", "two deliberate
  non-edges"), `crates/repark-python/map.md` and `ARCHITECTURE.md`.
- **Size.** About ten lines of code plus the policy row and prose. `repark-sql` (12,747 lines of
  source in 44 files) links into the wheel.
- **What moves.** Nothing. The binding names `repark_sql::AnsiDialect`.
- **Crate DAG.** One new tier-4 → tier-3 normal edge. The layering rule allows it once it is
  declared. It reverses the design's "no ANSI surface from Python".
- **Never-worse list.** §3.4: 11 statements in 5 classes, plus 3 pins on text.
- **Risks.** The ANSI door's own defects become `repark.sql()` defects (N1, N2, N4). Python users
  see new refusal texts and classes. `NativeDoorTypes` keeps no planner code to move; measured, a
  `NativeDoorTypes` left as it is changes no answer (§3.1), so ruling 113's "carries no planner
  code" holds.

### (B1) Move the dialect or the router down

- `AnsiDialect::execute` is `router::execute`, and the router is the door: 12,747 lines in 44
  files (`create_table`, `alter`, `merge`, `ref_ddl`, `schema_ddl`, `time_travel`, `truncate`,
  `insert_overwrite`, `session_insert`, `pg_insert`, `update_cast`, `guards`, `sniff`,
  `refusals`, `declared_refuse`, …).
- Into `repark-core`, the engine crate holds a door and `repark-sql` becomes a shell. Into
  `repark-spark`, one crate holds both doors, which the two-doors rule forbids.
- The never-worse list is the list of (A). This is an architecture change, not a unit.

### (B2) Move the router's refusal down (recommended, as the stopgap)

- **What moves.** The ANSI copy of the G3-E8 refusal: `DmlSubqueryVerb`, the statement guard,
  `delete_target`, `update_target` and the message (`guards.rs` 447–569). It moves to a new
  module `crates/repark-iceberg/src/write/predicate_dml/subquery_where.rs`, beside the allow-lists
  it consults. It reuses `predicate_dml`'s own probe (`expression_contains_subquery`), so the ANSI
  probe copy goes. `object_name_of` stays in `guards.rs`; two other guards use it. Relocated code
  sheds its comments.
- **The one signature.** `refuse_dml_subquery_predicate(statement: &Statement,
  identity_forms_execute: bool) -> Result<()>`. The ANSI router passes `true` (its identity path
  answers the allow-listed forms). `DataFusionDialect` passes `false` (it has no identity path, so
  every subquery form refuses).
- **Files.** The new module and its tests; `predicate_dml.rs` (one `pub mod` line, 989 → 990 of
  the 1,000-line default ceiling); `guards.rs` (about 110 lines out); `router.rs` (two call sites);
  `crates/repark-sql/src/guards/tests.rs` (five call sites change path, assertions unchanged);
  `crates/repark-core/src/dialect.rs` (`DataFusionDialect::execute` parses the statement with the
  session's parser dialect and calls the guard, about ten lines); the pins of §9; the maps of the
  touched directories.
- **Crate DAG.** No change: `repark-core → repark-iceberg` and `repark-sql → repark-iceberg` exist.
  `repark-core`'s `lib.rs` is at its 155-line ceiling, so the home is not a new `repark-core`
  module.
- **Copies.** Two, as today: the shared home and the Spark SQL door's. Probe copies go from three
  to two.
- **Never-worse list.** §3.6: the two coincidence shapes; U4 and D4 change refusal text.
- **Risks.** The native door parses each statement twice (a parse costs little next to planning;
  step 0 measures it). The G3-E8 hint says "Rewrite it as MERGE INTO …", and `MERGE` refuses on
  `repark.sql()` today (Q5). `repark.sql()` refuses the five forms the other doors answer until
  routing lands (Q6). Every Rust session with the default dialect gains the refusal; those sessions
  have the same defect today.

### (C) A refusal on the `repark.sql()` door only

- It cannot call the ANSI door's refusal: `guards::refuse_dml_subquery_predicate` is `pub(crate)`
  in `repark-sql`, and `repark-python` has no `repark-sql` edge.
- **(C1)** A copy in the binding or in `repark-core` is the third copy the card forbids.
- **(C2)** The binding calls the Spark SQL door's copy. It is `pub(crate)`, so `repark-spark`'s
  public surface grows. Its statement-level function exempts the allow-listed forms, which on the
  native door would reach DataFusion and write every row (U1 and D1 stay wrong). The low-level
  function would need the binding to take the statement apart: guard logic in the thin adapter, and
  the native door tied to the Spark door's target rendering.
- So the stopgap is (B2).

### (D1) A plan-level refusal in `PreExecute::guard`

- Refuse a `Dml(Update)` or `Dml(Delete)` whose input `Filter` holds `InSubquery`, `Exists` or
  `ScalarSubquery`. Ruling 90's `SET` refusal already lives there (`pre_execute.rs` 66–88), and
  every door's delegate path calls `guard`.
- Beside the two AST copies it is a third copy. As their replacement it is one copy, but it moves
  the refusal after the merge-on-read and update-cast valves on the ANSI door and renders the target
  from the plan's `TableReference`: a rewrite on two doors, its own unit.

### (D2) The real fix, every door

- Execute subquery predicates correctly: widen the identity path (`execute_predicate_dml`, a
  `SELECT` over pinned `(_file, _pos)`) to any predicate, or rewrite to the RePark `MERGE`
  executor. This is the UPDATE-SUBQUERY-STORE-1 family, out of 106's scope.

## 5. Why routing is more than one unit

- The edge (A) is an owner ruling (ruling 113, `Cargo.toml` owner-only). The only alternative,
  (B1), moves the whole door.
- Routing makes `repark.sql()` worse on N1, N2 and N4. Each is an ANSI-door defect that the Rust
  door has today; each needs its own fix and pins before routing is never worse.
- Routing turns five pinned expectations on main red: three on text (one of them a helper that
  four tests share) and two on N1 and N4. It also changes 17 refusal texts and 13 result shapes
  that need dated re-pins.

The card's stopgap condition ("if routing is more than one unit carries, a refusal on that door is
the stopgap and routing stays this card") therefore applies.

## 6. Recommendation

- **106 builds (B2).** The G3-E8 refusal lives once for the ANSI door and `repark.sql()`, in
  `repark-iceberg`'s `write::predicate_dml`. `DataFusionDialect` refuses every `UPDATE` or `DELETE`
  whose `WHERE` holds a subquery. The 22 pins of §9 go from red to green; nothing else on any door
  changes.
- **Routing stays on the card** with (A) as its mechanism, after the owner disposes of N1, N2, N4
  and the pins of §3.4 (Q2, Q3). When routing lands, the ANSI router's call with `true` replaces
  the native door's `false`, and U1, D1, D3, D5 and D6 answer Spark's rows on `repark.sql()`.
- **The Spark SQL door's copy** stays where it is in 106 (Q7).

## 7. The dialect line

**Today** (`crates/repark-python/src/session.rs`, `PyReparkSession::native`, line 160):

```rust
.with_extension(Arc::new(crate::session_runtime::NativeDoorTypes));
```

**After 106, as recommended.** The line does not change. The refusal rides in the builder's
default dialect, `repark_core::DataFusionDialect`.

**After U-5** (its branch `d38ac00b`):

```rust
.with_extension(Arc::new(crate::session_runtime::NativeDoorTypes))
.with_local_fs_guard();
```

- `build()` wraps the dialect it holds, `DataFusionDialect`, in `LocalFsGuardedDialect`. The
  wrapper's `execute` refuses local-file statements, then calls `inner.execute`, so the 106
  refusal still runs.
- The type planner comes from `NativeDoorTypes`: the extension's planner wins over the dialect's
  (`crates/repark-core/src/session/df_guards.rs` line 77). So the Q6 wall type survives the guard.

**Under (A), when routing lands:**

```rust
.with_sql_dialect(Arc::new(repark_sql::AnsiDialect))
.with_extension(Arc::new(crate::session_runtime::NativeDoorTypes))
.with_local_fs_guard();
```

- The `register_native_door_functions` call after `finish_session` goes.
- The guard wraps `AnsiDialect`, which keeps its own SEC-02 gate.
- `NativeDoorTypes` may stay as it is: §3.1 measured no difference.

## 8. The cells every later merge re-runs

Ruling 113: every rebase in the order re-runs the Q6 cell and the local-file cell. 106 adds its
own cell, because U-5's wrapper sits in front of the dialect that carries the refusal.

| Cell | Statement on `repark.sql()` | Main | After 106 | After U-5 | Where it is pinned |
|---|---|---|---|---|---|
| Q6 | `SELECT CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP) AS a, TIMESTAMP '…' AS b, CAST('…' AS TIMESTAMPTZ) AS c` | `timestamp[us]`, `timestamp[us]`, `timestamp[us, tz=UTC]`, value `…05.123456` | same | same | `test_ice_tsns_type_planner_1.py::test_repark_sql_plans_timestamp_as_the_wall[q6\|repark_sql]` |
| Local file | `COPY (SELECT 1 AS a) TO '<tmp>/outside/x.csv' STORED AS CSV`; control: the same `COPY` to a path under the warehouse | written; control written | same | refused with U-5's native text, nothing written; control written | U-5's `test_pa_local_fs_policy_1.py`; the Spark SQL door's twins in `test_sec_boundaries.py` (`test_copy_to_local_outside_warehouse_refuses_by_default`, `test_copy_to_inside_registered_warehouse_is_grandfathered`) |
| 106 | P-U1 and P-D1 of §9 | every row written; table emptied | G3-E8 refusal, rows and snapshots kept | same | `test_repark_sql_update_subquery_where_1.py` |

## 9. The build: steps and pins

**Step 0, measure, no product code.** On the build's base: the 22 pin statements on
`repark.sql()` (rows and snapshot count), the same statements on the ANSI door and the Spark SQL
door, the three cells of §8, and the cost of the second parse (1,000 `SELECT 1` calls through
`repark.sql()`, median of five runs, head against base).

**Step 1, pins, red on main.** `python/repark/tests/test_repark_sql_update_subquery_where_1.py`,
seeded as §2.1. Each pin asserts the G3-E8 needle "subquery predicates are silently mis-executed"
with the statement's verb, the rows unchanged, and, for an Iceberg target, the snapshot count
unchanged.

| Pin | `UPDATE … SET v = 5 WHERE` / `DELETE … WHERE` | Main (red) |
|---|---|---|
| P-U1 / P-D1 | `id IN (SELECT id FROM ice.ns.s)` | every row written / table emptied |
| P-U2 / P-D2 | `id = (SELECT max(id) FROM ice.ns.s)` | every row written / table emptied |
| P-U3 / P-D3 | `EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)` | every row written / table emptied |
| P-U4 / P-D4 | `v = (SELECT s.w FROM ice.ns.s WHERE s.id = t.id)` | DataFusion's planner text |
| P-U5 / P-D5 | `id NOT IN (SELECT id FROM ice.ns.s)` | every row written / table emptied |
| P-U6 / P-D6 | `EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)` | every row written / table emptied |
| P-U7 / P-D7 | `id IN (SELECT id FROM ice.ns.s) AND v > 25` | ids 3 and 4 written / deleted |
| P-U8 / P-D8 | `id = ANY (SELECT id FROM ice.ns.s)` | every row written / table emptied (measured for `UPDATE`; step 0 measures `DELETE`) |
| P-U9 / P-D9 | the card's unqualified spelling (C1, C2) | every row written / table emptied |
| P-U10 / P-D10 | the in-memory table `mem3`, `a IN (SELECT 1)` | every row written / deleted |
| P-C1 | `DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.t)` | answers (right by coincidence) |
| P-C2 | `UPDATE ice.ns.t SET v = 5 WHERE EXISTS (SELECT 1 FROM ice.ns.s)` | answers (right by coincidence) |

- **Controls, green on main and on the head:** `WHERE id = 2` and `WHERE id IN (1, 3)` for both
  verbs; ruling 90's `SET` refusal (`test_update_set_subquery_walk_1.py` stays green); a `SELECT`,
  an `INSERT … SELECT` and a CTAS with `WHERE id IN (SELECT …)`; the cells of §8; the 46
  identical statements of §3.
- **Rust pins.** In `repark-iceberg`: the shared guard with `true` lets the allow-listed forms
  pass, with `false` refuses them, and its text is byte-identical to today's ANSI text for one
  `UPDATE`, one `DELETE` and the `<table>` fallback. In `repark-core`: a `DataFusionDialect`
  session over a memory catalog refuses P-U1 and P-D1 and keeps the rows.
- **Pins that must not move:** the G3-E8 tests in `crates/repark-sql/src/guards/tests.rs` (only
  the call path changes) and `crates/repark-sql/src/guards/ansi_door.rs`;
  `crates/repark-sql/tests/ansi_update_subquery_refusal.rs`; `crates/repark-spark/src/tests/normalize.rs`;
  `python/repark/tests/test_dml_subquery_parity.py`.
- **Mutants for the verifier:** drop the native call (every P-* red); pass `true` on the native
  door (P-U1, P-D1, P-D3, P-D5, P-D6 and P-C1 write rows); drop the parse fallback (a parse error
  changes text).

**Step 2, the move.** The shared module of §4 (B2); the ANSI router and its tests call it with
`true`; the ANSI pins stay green byte for byte.

**Step 3, the native door.** `DataFusionDialect::execute` parses the statement with the session's
parser dialect. For a parsed `UPDATE` or `DELETE` it calls the shared guard with `false`. On a
parse failure it falls through to `PreExecute::run`, so a parse error keeps its text. The pins go
green.

**Step 4, records.** The card and the registry row name `DELETE`, the pins, and the stopgap, and
keep routing open. The 1.5.6 draft turns the known-issue bullet into a behaviour line: `repark.sql()`
refuses an `UPDATE` or `DELETE` with a subquery in `WHERE`, where it wrote every row. The unit
ledger; the maps of every touched directory.

**Gates.** `cargo test -p repark-iceberg -p repark-core -p repark-sql -p repark-spark` inside the
build lock; a release module build and the new and touched pytest files; `make rust-clippy`,
`make rust-panic-ban`, `cargo fmt --all -- --check`; `make check-rust-file-size check-crate-dag
check-lib-rs`; the map, docs-link, ledger and spelling gates; the comment gate at 0.

## 10. Halts for the build

- **H1.** Any crate edge, or any change to `Cargo.toml` or `Cargo.lock`.
- **H2.** A ceiling raise: `predicate_dml.rs` past 1,000 lines, or any crate root.
- **H3.** A third copy: the G3-E8 text or a subquery probe anywhere except the shared home and the
  Spark SQL door's existing copy.
- **H4.** An existing pin changes text, assertion or location (the list in §9).
- **H5.** A `repark.sql()` statement outside the refused class changes answer or error text
  (the 46 identical statements of §3 are the check), or a coincidence shape appears beyond P-C1
  and P-C2.
- **H6.** The second parse costs more than 2% on the step-0 measure.
- **H7.** U-5 merges first and its wrapper does not call `inner.execute`.
- **H8.** The shared guard needs a door-specific target renderer (the Spark copy's case, Q7).
- **H9.** Any need for a `repark-python → repark-sql` edge.

## 11. Questions

- **Q1 (ruling).** *Premise:* routing needs the edge of (A) or the door move of (B1), and makes 11
  statements worse in 5 classes (§3.4, §5). *Question:* does 106 build the stopgap (B2) and leave
  routing on the card? *Lean:* yes.
- **Q2 (owner).** *Premise:* under today's DAG, routing has two mechanisms: (A), one dependency
  line in an owner-only manifest and a reversed design non-edge; or (B1), the whole door moved.
  *Question:* which mechanism does routing use when it is scheduled? *Lean:* (A), after Q3's items
  close; (B1) changes the architecture.
- **Q3 (ruling).** *Premise:* N1 (bare-name in-memory tables), N2 (`TIMESTAMP` and `TIMESTAMPTZ`
  columns in `CREATE TABLE`) and N4 (internal kernels by name) are ANSI-door defects that routing
  hands to `repark.sql()`; N3 is moot once U-5 lands; N7, the 17 texts and the 13 result shapes are
  changes. *Question:* how does each close before routing? *Lean:* N1, N2 and N4 get their own
  cards and are fixed on the ANSI door first; N7, the texts and the shapes become dated re-pins with
  one release-note line.
- **Q4 (ruling).** *Premise:* the card, the registry row and the 1.5.6 note name `UPDATE`; on
  `repark.sql()` 6 of the 8 `DELETE` forms of §2.2 empty the table and a 7th deletes a row that
  does not match. *Question:* does the stopgap cover `DELETE`? *Lean:* yes; the build amends the
  three records to name it.
- **Q5 (ruling).** *Premise:* the G3-E8 text is byte-identical on the two doors; its hint, "Rewrite
  it as MERGE INTO …", refuses on `repark.sql()` until routing. *Question:* which text does
  `repark.sql()` raise? *Lean:* the G3-E8 text verbatim, one text for the family as ruling 95 asked
  for the `SET` family; the 1.5.6 note already sends readers to the Spark SQL session.
- **Q6 (ruling).** *Premise:* the two doors answer U1, D1, D3, D5 and D6 through the identity
  path; `repark.sql()` has none, so the exemption would send those forms to DataFusion, which writes
  every row. *Question:* does `repark.sql()` refuse them too? *Lean:* yes, until routing lands.
- **Q7 (ruling).** *Premise:* the Spark SQL door's copy renders a backtick-quoted target in double
  quotes and has five call sites of its own; 106 is a fix, and AGENTS keeps fixes narrow.
  *Question:* does the Spark copy fold into the shared home in 106? *Lean:* no; a later tidy unit
  folds it, with the renderer as a parameter.

## 12. Observed, out of scope

- **The G3-E8 defect is wider than its text.** It hits DataFusion's in-memory tables as well as
  the Iceberg writer (§2.2), and through `repark.sql()` the `DELETE` forms empty the table.
- **The ANSI door's local-file opt-in cannot open.** `allow_local_filesystem_ddl`
  (`crates/repark-sql/src/guards.rs` line 331) looks for `repark.sql.allow_local_filesystem_ddl`
  in `ConfigOptions::entries()`. DataFusion 54.1's `extensions_options!` lists an extension field
  under its bare name, and a session without `SparkExtension` has no `repark.sql` extension at all.
  Measured: `repark.sql.allowLocalFilesystemDDL = true` on the builder still refuses, with and
  without an extension that attaches the conf. No ANSI test opens the gate.
- **`SET repark.sql.… = …` fails on `repark.sql()` and on the ANSI door** with "Could not find
  config namespace \"repark\"": DataFusion splits the key at the first dot, and the extension's
  prefix has two segments.
- **ANSI `CREATE TABLE` maps a bare `TIMESTAMP` and `TIMESTAMPTZ` to nanoseconds and refuses.**
  Ruling 94's Q6 contract types `TIMESTAMP` as microseconds on that door for expressions; the
  registry's residue R-17 dates only `TIMESTAMP WITH TIME ZONE`.
- **The ANSI door registers internal kernels by name** (`__repark_cast_timestamp_ns__`,
  `__repark_cast_timestamp_ntz__`, `__repark_store_*`), so SQL text resolves them; the r013 cell
  pins the opposite on `repark.sql()`.
- **A missing three-part `DELETE` target on the ANSI door** answers a raw
  `TableNotFound => No such table: TableIdent { … }`.
- **Result shapes on the ANSI door differ by path:** delegated DML answers a `count` row, the
  identity path answers none, and DDL answers one zero-column row.
- **The Rust Spark session refuses C1 and C2**, which the facade answers after `USE ice.ns`. The
  cause is not investigated.
- **Spark commits a snapshot when a subquery predicate matches no row** (U6 and D6: 1 → 2).
