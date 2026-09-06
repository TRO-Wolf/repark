# Unit ledger — EX-21 · v1.1 example backfill, the `Catalog.*` remainder and the `SparkSession` surface (a)

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-21 merges, or when the owner closes the
slate row.

**Unit:** EX-21 · **Date:** 2026-09-04 · **Model:** glm-5.3-flash · **Branch:** `docs/ex-21-catalog-session` · **Base:** `b5b17f0`
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-21 lane brief (35 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/catalog/`, `docs/examples/session/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_window_catalog.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the 10 `Catalog.*` names still uncovered after EX-20 plus 25 `SparkSession` names at
base `b5b17f0` (camelCase and snake_case aliases are one example each, both names in `COVERS`).
Sixteen files cover the 34 names the live oracle measured Spark-equal or extension-honest:
9 `Catalog.*` and 25 `SparkSession`. `Catalog.list_databases` stays on the backlog: it is the
**same function object** as the divergent `listDatabases` (§7 `EX-CAT-2`, FA-2 field shapes), so
covering it would paper that row over — the snake spelling joins the EX-CAT-2 note rather than
getting a new row. Five measured divergent arms are filed as §7 `EX-SES-1` (`registerFunction`
answers the `UserDefinedFunction` object where Spark's deprecated alias returns the original
callable), §7 `EX-SES-2` (an action on a `newSession()` result promotes it process-active where
Spark keeps the caller active; the Spark column re-measured and corrected in round 2 — the
spare's `stop()` clears the active slot on both engines), §7 `EX-SES-3` (`createDataFrame([], [names])`
answers an empty string-typed frame where Spark raises `[CANNOT_INFER_EMPTY_SCHEMA]`), §7
`EX-SES-4` (`conf.get` on an unset key raises a bare `Exception` where Spark raises
`SparkNoSuchElementException` `[SQL_CONF_NOT_FOUND]`), and §7 `EX-SES-5` (a missing path through
the readers raises `AnalysisException` on both engines with different texts), all pinned in
`python/repark/tests/test_examples_window_catalog.py`; the names land on their agreeing arms.
`SparkSession.registerTempTable` and `SparkSession.pandas_api` are repark-only names on live
PySpark 4.1.2 (`hasattr` False) whose entire contract is the loud
`UnsupportedOperationException`; they are covered by refusal examples that assert the exception
and the supported route named in its message. `SparkSession.read_excel` / `excel_sheet_names` are
out of this batch per the brief. Every snake_case spelling measured `hasattr` `False` on live
PySpark 4.1.2 and is covered as a repark extension beside its camelCase twin or as an
extension-only surface. Examples use the local memory catalog, temp views, and files the example
itself writes (no network, no JVM, no cloud).

**Roster (35):** `Catalog.list_databases`, `Catalog.list_tables`, `Catalog.registerFunction`,
`Catalog.register_function`, `Catalog.setCurrentCatalog`, `Catalog.setCurrentDatabase`,
`Catalog.set_current_catalog`, `Catalog.set_current_database`, `Catalog.tableExists`,
`Catalog.table_exists`, `SparkSession.Builder.app_name`, `SparkSession.Builder.config`,
`SparkSession.Builder.get_or_create`, `SparkSession.Builder.master`, `SparkSession.active`,
`SparkSession.catalog`, `SparkSession.conf`, `SparkSession.create_dataframe`,
`SparkSession.create_namespace`, `SparkSession.display_style`, `SparkSession.getActiveSession`,
`SparkSession.list_df_schema_table_names`, `SparkSession.list_iceberg_table_names`,
`SparkSession.list_temp_view_names`, `SparkSession.newSession`, `SparkSession.pandas_api`,
`SparkSession.range`, `SparkSession.read_csv`, `SparkSession.read_json`,
`SparkSession.read_parquet`, `SparkSession.read_iceberg_table`,
`SparkSession.registerTempTable`, `SparkSession.register_memory_catalog`,
`SparkSession.resolve_table_name`, `SparkSession.refresh_catalog_provider`.

**Grouping (16 files, each named for one breath):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `catalog/set_current_names.py` | `Catalog.setCurrentCatalog`, `Catalog.set_current_catalog`, `Catalog.setCurrentDatabase`, `Catalog.set_current_database` | The current-catalog/database setters: register a memory catalog (currentCatalog flips to it), set both spellings back and forth, read each value back. |
| `catalog/table_exists.py` | `Catalog.tableExists`, `Catalog.table_exists` | Temp view True, missing name False, both spellings. |
| `catalog/register_function.py` | `Catalog.registerFunction`, `Catalog.register_function` | Register a scalar UDF through the catalog, probe with `functionExists`, answer inside SQL, both spellings. |
| `catalog/list_tables.py` | `Catalog.list_tables` | The exact `MANAGED` Iceberg row, the `TEMPORARY` view row, the bare arm, an exact-pattern arm. |
| `session/builder.py` | `SparkSession.Builder.app_name`, `SparkSession.Builder.master`, `SparkSession.Builder.config`, `SparkSession.Builder.get_or_create` | The snake_case builder chain with the app name, master, and shuffle-partition values read back through `conf` / `sparkContext`. |
| `session/session_state.py` | `SparkSession.active`, `SparkSession.getActiveSession`, `SparkSession.newSession` | The active-session trio: builder session active, spare distinct and answering, no active-slot theft before an action. |
| `session/session_conf.py` | `SparkSession.conf` | String and bool round-trips through the runtime conf map, plus the unset-key default. |
| `session/session_catalog.py` | `SparkSession.catalog` | The `Catalog` type and the untouched default names. |
| `session/frame_builders.py` | `SparkSession.create_dataframe`, `SparkSession.range` | Row-list frames, the explicit-schema empty frame, and the exclusive `range(start, end[, step])` with the negative step. |
| `session/read_files.py` | `SparkSession.read_csv`, `SparkSession.read_json`, `SparkSession.read_parquet` | One local file per format, written by the example (CSV with explicit `header`), plus the missing-path `AnalysisException` arm. |
| `session/register_catalog.py` | `SparkSession.register_memory_catalog`, `SparkSession.create_namespace` | The registered catalog lists, becomes current, hosts a namespace. |
| `session/iceberg_tables.py` | `SparkSession.read_iceberg_table`, `SparkSession.list_iceberg_table_names`, `SparkSession.list_df_schema_table_names`, `SparkSession.refresh_catalog_provider` | One CTAS table: live listing, table read, provider directory, refresh, re-read. |
| `session/temp_views.py` | `SparkSession.list_temp_view_names` | Bare session lists none; two created views both list. |
| `session/resolve_names.py` | `SparkSession.resolve_table_name` | Bare and two-part qualification, the temp-view home, the plain form. |
| `session/display_style.py` | `SparkSession.display_style` | The `spark` default, the `polars` switch, the `conf` mirror. |
| `session/legacy_refusals.py` | `SparkSession.registerTempTable`, `SparkSession.pandas_api` | Both refuse loud with `UnsupportedOperationException` naming the supported route. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Sixteen files under `docs/examples/catalog/` and `docs/examples/session/` land runnable local examples for the 34 roster names the live oracle measured Spark-equal or extension-honest, every asserted value measured against live PySpark 4.1.2 before it was written; those 34 leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 34, 411 → 377 on the dispatch base and 374 → 340 on the shipped tree after the EX-20 merge, with no other `scripts/` change; `Catalog.list_databases` stays on the backlog sharing §7 `EX-CAT-2`, and the five measured divergent arms are §7 `EX-SES-1`..`EX-SES-5` pinned in `python/repark/tests/test_examples_window_catalog.py`; no product file is touched; the gate's static half and its `--require-execute` leg both exit 0. | Red-first capture (34 findings before, 0 after), the oracle table (35 rows, one per roster name) plus the round-2 table (one row per newly measured arm), the sixteen scripts each exit 0, and the recorded gate exit codes. | **PROVEN** |

`LOGIC_SCORE` = **1/1 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

Captured at `b5b17f0` (the base, with the sixteen example files held outside the tree), in this
tree: the 34 roster rows were deleted and `BACKLOG_BASELINE` lowered to 377 in place, measured,
then restored with `mv` before any committed state. At the base — the 35 roster rows still in
`docs/examples/backlog.txt`, `BACKLOG_BASELINE=411` — `python3 scripts/check_example_coverage.py
--skip-execute` exits **0** (`913 public names; 500 covered; 411 backlog; 2 exceptions; 130
examples`). **Provocation:** delete the 34 Spark-equal/extension-honest roster rows and lower
`BACKLOG_BASELINE` to 377 (`411 − 34`) with no example files present; the same gate exits **1**
with exactly 34 findings, one per roster name and no others (`Catalog.list_databases` stayed
listed, so it produced no finding). With the sixteen files present, the 34 names removed and
`BACKLOG_BASELINE=377`, the gate exits **0** (`534 covered; 377 backlog; 146 examples`).

## Oracle (live PySpark 4.1.2, ANSI on, local[2], JDK zulu-17, TZ=UTC)

Measured with `.venv/bin/python` and `JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, throwaway scripts
under `scratch/ex21-oracle/` (gitignored, never committed): leg 1 the `hasattr` classification
(JVM-free), leg 1b every repark arm (JVM-free), leg 2 every Spark-native arm on one live Spark
JVM (`spark.sql.warehouse.dir` in a tempdir, `local[2]`, ANSI on, UTC). The Spark `setCurrentCatalog`
second-catalog arm needs a catalog plugin class outside the plain distribution (the in-memory
`InMemoryTableCatalog` is test-scope), so both engines were measured on the default
`spark_catalog` round-trip; repark's registered-catalog flip is extension behavior. The
`listTables`/`list_tables` comparison rows: Spark `('ex21_t', 'spark_catalog', ['default'], None,
'MANAGED', False)`; repark `('ex21_t', 'ex21_cat', ['ex21_db'], None, 'MANAGED', False)` — the
same shape over each engine's own fixture. Fixtures: `[(1,"x")]` temp-view frame; CTAS
`ex21_t AS SELECT 1 AS id UNION ALL SELECT 2 AS id`; CSV `k,v\na,1\nb,2\n`; NDJSON
`{"k":"a","v":1}` lines; `[(1,"a"),(2,"b")]` Parquet frame.
`pins: ex-21-catalog-session/C-001`

### Round 2 measured cells (2026-09-04, same oracle settings; one Spark JVM, EX-SES-2 stop arm last)

| Arm | Spark (repr) | repark (repr) | Verdict |
|---|---|---|---|
| `newSession` spare action, active slot | `getActiveSession() is spark` → `True` after `spare.sql(...).collect()` | `getActiveSession() is spare` → `True` after the same action | divergence (§7 `EX-SES-2`) |
| `newSession` spare `stop()`, active slot | `getActiveSession()` → `None` | `getActiveSession()` → `None` | agree |
| `newSession` spare `stop()`, caller | `spark.sql(...)` raises `AttributeError` (`'NoneType' object has no attribute 'setCallSite'` — the shared SparkContext stopped) | `repark.sql(...)` answers `[(2,)]` | Spark-side consequence of the shared context; active-slot semantics agree, the row stays scoped to the promotion |
| `createDataFrame([], ["a"])` | raises `PySparkValueError` `[CANNOT_INFER_EMPTY_SCHEMA]` | `collect()` `[]`, `dtypes` `[('a', 'string')]` | divergence (§7 `EX-SES-3`) |
| `createDataFrame([], schema="a int")` | `collect()` `[]`, `dtypes` `[('a', 'int')]` | same | agree — taught |
| `conf.get("ex21.unset.key")` | raises `SparkNoSuchElementException` `[SQL_CONF_NOT_FOUND]` | raises bare `Exception` (`type(...) is Exception`), "Configuration property … is not set." | divergence (§7 `EX-SES-4`) |
| `conf.get("ex21.unset.key", "fallback")` | `'fallback'` | `'fallback'` | agree — taught |
| `read_csv`/`read_json`/`read_parquet` on a missing file (format's extension) | `AnalysisException` `[PATH_NOT_FOUND]` "Path does not exist: …", eager at reader construction | `AnalysisException` "Error during planning: No files found at …" | same type, text diverges (§7 `EX-SES-5`) |
| `read_parquet` on a missing *directory* | `AnalysisException` `[PATH_NOT_FOUND]` | `PySparkException` (extension mismatch) — measured, kept out of the example and the row | taught arm avoids the directory shape |
| `range(5, 0, -1)` | `[5, 4, 3, 2, 1]` | `[5, 4, 3, 2, 1]` | agree — taught |
| `resolve_table_name("default.<t>")` | n/a (extension) | `'spark_catalog.default.<t>'` | extension arm — taught |

| Name | Spark value (repr) | repark value (repr) | Kept / dropped | File | Note |
|---|---|---|---|---|---|
| `Catalog.list_databases` | `[('default', 'spark_catalog', 'default database', 'file:<warehouse>')]` | `[('ex21_db', 'ex21_cat', None, None)]` — None fields | dropped | §7 `EX-CAT-2` | same function object as the divergent `listDatabases`; the snake spelling joins that row, stays on the backlog |
| `Catalog.list_tables` | `listTables` db arm `[('ex21_t', 'spark_catalog', ['default'], None, 'MANAGED', False), ('ex21_tv', None, [], None, 'TEMPORARY', True)]`; exact pattern `[('ex21_t', …)]` | db arm `[('ex21_t', 'ex21_cat', ['ex21_db'], None, 'MANAGED', False), ('ex21_tv', None, [], None, 'TEMPORARY', True)]`; bare `[('ex21_tv', None, [], None, 'TEMPORARY', True)]`; pattern `[('ex21_t', …)]` | kept | `catalog/list_tables.py` | snake extension of the Spark-equal `listTables` |
| `Catalog.registerFunction` | answers the callable `f` (`type(…).__name__ == 'function'`); `functionExists` True; SQL `('u4',)` | answers `UserDefinedFunction`; `functionExists` True; SQL `('u4',)` | kept | `catalog/register_function.py` | return-value arm is §7 `EX-SES-1`, pinned, not taught |
| `Catalog.register_function` | `hasattr` False (extension) | same existence and SQL answers | kept | `catalog/register_function.py` | snake spelling |
| `Catalog.setCurrentCatalog` | set `'spark_catalog'` → readback `'spark_catalog'`; return `None` | registered-catalog flip `'ex21_cat'`, both spellings read back; return `None` | kept | `catalog/set_current_names.py` | flip is extension behavior (plain Spark has no second catalog to set) |
| `Catalog.setCurrentDatabase` | `CREATE DATABASE ex21_db`; set → `'ex21_db'`; return `None`; set back `'default'` | same values over the memory catalog with `create_namespace` | kept | `catalog/set_current_names.py` | |
| `Catalog.set_current_catalog` | `hasattr` False (extension) | same readbacks | kept | `catalog/set_current_names.py` | snake spelling |
| `Catalog.set_current_database` | `hasattr` False (extension) | same readbacks | kept | `catalog/set_current_names.py` | snake spelling |
| `Catalog.tableExists` | temp view `True`; missing `False`; three-part `True`; two-part `True` | same | kept | `catalog/table_exists.py` | example keeps the temp/missing arms |
| `Catalog.table_exists` | `hasattr` False (extension) | same | kept | `catalog/table_exists.py` | snake spelling |
| `SparkSession.Builder.app_name` | `hasattr` False (extension); `appName` → `conf.get('spark.app.name')` = app name | same value via `app_name` | kept | `session/builder.py` | snake spelling |
| `SparkSession.Builder.config` | `config('spark.sql.shuffle.partitions', '4')` → `conf.get` `'4'` | same `'4'` | kept | `session/builder.py` | Spark-native spelling |
| `SparkSession.Builder.get_or_create` | `hasattr` False (extension); `getOrCreate` session | session object | kept | `session/builder.py` | snake spelling |
| `SparkSession.Builder.master` | `hasattr` False (extension); `master` → `sc.master` `'local[2]'` | `sc.master` `'local[1]'` | kept | `session/builder.py` | snake spelling; repark warns once (OTH-010) |
| `SparkSession.active` | `active() is spark` | same | kept | `session/session_state.py` | |
| `SparkSession.catalog` | `type(…).__name__ == 'Catalog'`; `currentCatalog` `'spark_catalog'` | same | kept | `session/session_catalog.py` | |
| `SparkSession.conf` | set/get `'v'`; bool `True` → `'true'`; unset key raises `SparkNoSuchElementException` `[SQL_CONF_NOT_FOUND]`; default form `'fallback'` | same set/get and bool; unset key raises a bare `Exception` (§7 `EX-SES-4`); default form `'fallback'` | kept | `session/session_conf.py` | example keeps the round-trips and the default arm; the unset-key error contract is pinned |
| `SparkSession.create_dataframe` | `hasattr` False (extension); `createDataFrame` rows `[(3, 'c'), (4, 'd')]`; empty name-list raises `[CANNOT_INFER_EMPTY_SCHEMA]`; empty explicit schema `[]` with `[('a', 'int')]` | same rows; empty name-list answers `[]` with `[('a', 'string')]` (§7 `EX-SES-3`); empty explicit schema `[]` with `[('a', 'int')]` | kept | `session/frame_builders.py` | snake spelling; example keeps the row-list and explicit-schema-empty arms; the name-list empty arm is pinned |
| `SparkSession.create_namespace` | `hasattr` False (extension) | namespace exists after creation (`databaseExists` True) | kept | `session/register_catalog.py` | extension |
| `SparkSession.display_style` | `hasattr` False (extension) | default `'spark'`; set `'polars'`; `conf.get` mirror | kept | `session/display_style.py` | extension |
| `SparkSession.getActiveSession` | the session; after `stop()` `None` | same | kept | `session/session_state.py` | |
| `SparkSession.list_df_schema_table_names` | `hasattr` False (extension) | `['ex21_t']` for the CTAS schema | kept | `session/iceberg_tables.py` | extension |
| `SparkSession.list_iceberg_table_names` | `hasattr` False (extension) | `['ex21_t']` | kept | `session/iceberg_tables.py` | extension |
| `SparkSession.list_temp_view_names` | `hasattr` False (extension) | bare `[]`; two views both listed | kept | `session/temp_views.py` | extension; listing order is not creation-ordered, the example asserts the sorted names |
| `SparkSession.newSession` | distinct object; same app name; caller stays active before **and after** the spare's action; `spare.stop()` clears the active slot (`None`) and stops the shared SparkContext | distinct; caller stays active only until the spare action (§7 `EX-SES-2`); `spare.stop()` clears the active slot (`None`) | kept | `session/session_state.py` | example keeps the no-action arms; promotion arm pinned |
| `SparkSession.pandas_api` | `hasattr` False on live PySpark 4.1.2 (extension) | `UnsupportedOperationException` naming `DataFrame.to_pandas` | kept | `session/legacy_refusals.py` | refusal taught as a refusal |
| `SparkSession.range` | `[1, 2, 3]`; `[0, 2, 4]`; `[5, 4, 3, 2, 1]` for `range(5, 0, -1)` | same, negative step included | kept | `session/frame_builders.py` | round 2 added the negative-step control |
| `SparkSession.read_csv` | `hasattr` False (extension); nearest Spark arm `read.csv(header=True, inferSchema=True)` rows `[('a', 1), ('b', 2)]`; missing path `AnalysisException [PATH_NOT_FOUND]` | `read_csv(options={'header': 'true'})` rows same; missing path `AnalysisException … No files found` (§7 `EX-SES-5`) | kept | `session/read_files.py` | the default-arm column naming is the `DataFrameReader.csv` surface (EX-9 family), avoided here; both engines raise `AnalysisException` on a missing path, the example teaches the type |
| `SparkSession.read_json` | `hasattr` False (extension); `read.json` rows `[('a', 1), ('b', 2)]`, schema `k string / v bigint`; missing path `AnalysisException [PATH_NOT_FOUND]` | same rows and schema; missing path `AnalysisException … No files found` (§7 `EX-SES-5`) | kept | `session/read_files.py` | |
| `SparkSession.read_parquet` | `hasattr` False (extension); `read.parquet` rows `[(1, 'a'), (2, 'b')]`; missing path `AnalysisException [PATH_NOT_FOUND]` | same; missing path `AnalysisException … No files found` (§7 `EX-SES-5`) | kept | `session/read_files.py` | |
| `SparkSession.read_iceberg_table` | `hasattr` False (extension); Spark's read of its own table `[('ex21_t' rows)]` `(1,), (2,)` | same rows from the memory-catalog table | kept | `session/iceberg_tables.py` | extension |
| `SparkSession.registerTempTable` | `hasattr` False on live PySpark 4.1.2 (removed pre-4.1.2) | `UnsupportedOperationException` naming `createOrReplaceTempView` | kept | `session/legacy_refusals.py` | repark-only legacy alias; refusal taught as a refusal |
| `SparkSession.register_memory_catalog` | `hasattr` False (extension) | `listCatalogs` `[('ex21_cat', None), ('spark_catalog', None)]`; `currentCatalog` flips `'ex21_cat'` | kept | `session/register_catalog.py` | extension |
| `SparkSession.resolve_table_name` | `hasattr` False (extension) | bare `'spark_catalog.default.ex21_report'`; two-part `'spark_catalog.default.ex21_report'`; temp-view home `'datafusion.public.ex21_tv'`; plain `'spark_catalog.default.ex21_tv'` | kept | `session/resolve_names.py` | extension |
| `SparkSession.refresh_catalog_provider` | `hasattr` False (extension) | returns `None`; the read answers after | kept | `session/iceberg_tables.py` | extension |

## Gates (2026-09-04, on this tree)

| Command | Exit |
|---|---|
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_window_catalog.py -q` | **0** (9 passed) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |
| `.venv/bin/ruff check docs/examples python/repark/tests` | **0** |
| `.venv/bin/ruff format --check docs/examples python/repark/tests` | **0** |

The system `python3` in this clone cannot import `repark._native`; the `--require-execute` leg
runs under `.venv/bin/python`, which resolves `repark` to the sibling checkout of the same base
SHA `b5b17f0` (expected for this lane). The static half also runs green under system `python3`
with `--skip-execute`.

Counts line (execute leg):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 534 covered; 377 backlog; 2 exceptions; 146 examples`

Before this unit: `500 covered; 411 backlog; 130 examples` (at `b5b17f0`). On this unit's own
tree before the merge: `534 covered; 377 backlog; 146 examples` (`BACKLOG_BASELINE` 411 → 377).
On the shipped tree, after the EX-20 merge: `571 covered; 340 backlog; 154 examples`
(`BACKLOG_BASELINE` 374 → 340) — exactly the 34 kept names on top of EX-20's 37.

## Review-gap table (round-1 and round-2 findings, resolved in-lane)

| Finding | Disposition |
|---|---|
| `register_memory_catalog` refused a relative warehouse path (`storage location … is not an absolute path`) | the four catalog-bearing examples build the warehouse with `Path.cwd() / name`, caught by the examples' own `SystemExit` on the first local run |
| `Catalog.register_function` with an int-returning lambda failed (`declared type string`) — the default returnType is string | the example uses string-returning lambdas; Spark's arm measured the same value; nothing asserted across the declared-type boundary |
| the first provocation run held aside `session/sql.py` too, so its 6 covers appeared as findings | redone holding aside exactly the sixteen EX-21 files: exit 1 with exactly 34 findings, then restored |
| `list_temp_view_names` answered reverse-creation order, not creation order | the example asserts the sorted names (a bare session lists none; two views list both) — no order claim is taught |
| the session example files were briefly left in `docs/examples/catalog/` by the first provocation restore | moved back before any commit; the gate's execute leg re-run green on the corrected layout |
| S1: the §7 `EX-SES-2` Spark column claimed "only `stop()` on the active session clears it" | re-measured on live Spark: `spare.stop()` clears the active slot too (the shared SparkContext stops); the row rewritten to the true divergence only — Spark keeps the caller active after the spare's action, repark promotes the spare |
| S2: three unfiled divergences (empty name-list `create_dataframe`, unset-key `conf.get`, missing-path readers) | measured on both engines, filed as §7 `EX-SES-3`/`EX-SES-4`/`EX-SES-5`, pinned, and each agreeing arm taught in `frame_builders.py` / `session_conf.py` / `read_files.py` |
| S2: `range(5, 0, -1)` unmeasured negative-step control | measured `[5, 4, 3, 2, 1]` on both engines and taught in `frame_builders.py` |
| S3: `resolve_names.py` promised a two-part-qualified arm it did not hold | the arm added (`"default.ex21_report"` → `'spark_catalog.default.ex21_report'`, measured); the ledger and `session/map.md` claims are now true |
| S3: `session_catalog.py` module docstring claimed a conf surface it does not teach | docstring drops "and conf" |
| S3: `register_function.py` variable names did not match the arm they held (`snake_exists` held the camel arm) | renamed to `snake_exists`/`snake_rows` and `camel_exists`/`camel_rows` |

## Cost

The GLM (glm-5.3-flash) leg started 2026-09-04: read the contract, the corpus (EX-20 batch end to
end from `FETCH_HEAD`), and the gate; ran three oracle legs (leg 1 `hasattr` JVM-free, leg 1b
every repark arm JVM-free, leg 2 every Spark-native arm on one Spark JVM); wrote the sixteen
example files, the two divergence pins, the registry rows, the backlog ratchet and the maps, then
committed in slices; merged `origin/main` before hand-back with the backlog as the intersection of
both sides and the baseline at main's value minus 34. Base `b5b17f0`.

Round 2 (critic FAIL: one S1, four S2, three S3): re-measured the EX-SES-2 stop arm on one Spark
JVM (the spare's `stop()` clears the active slot on both engines; the first Spark-column claim was
wrong), measured and filed `EX-SES-3`/`EX-SES-4`/`EX-SES-5`, pinned all three, taught the
agreeing arms plus the negative-step `range` control, added the `resolve_names.py` two-part arm,
and fixed the two docstring/variable-name S3s. No backlog change (the divergent arms belong to
already-covered names; the counts stay 571/340/154).

## Disk

Pickup: `df -h` 665 GB free of 1.8 TB. The oracle scratch lives under the gitignored `scratch/`
(leg scripts plus a held-aside copy of the examples during the provocation, removed after);
Spark's `spark-warehouse` residue lands in the oracle tempdirs, not the repo. `.venv` and the
sibling-checkout native module reused; no cargo build, `make develop` not run.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-21 moves only the inventory/backlog ratchet and example files; it moves no wire,
and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-21-catalog-session
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 34 Spark-equal or extension-honest roster names are covered by sixteen new example files and the oracle table records both engines' values per name, all 35 roster rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/catalog/set_current_names.py, docs/examples/catalog/table_exists.py, docs/examples/catalog/register_function.py, docs/examples/catalog/list_tables.py, docs/examples/session/builder.py, docs/examples/session/session_state.py, docs/examples/session/session_conf.py, docs/examples/session/session_catalog.py, docs/examples/session/frame_builders.py, docs/examples/session/read_files.py, docs/examples/session/register_catalog.py, docs/examples/session/iceberg_tables.py, docs/examples/session/temp_views.py, docs/examples/session/resolve_names.py, docs/examples/session/display_style.py, docs/examples/session/legacy_refusals.py]
    - id: AT-2
      status: ATTACKED
      evidence: A COVERS name on a wrong receiver is unused and red (the classmethod covers bind through the repark-rooted session local, not the class); the backlog is an exact baseline 377 with the divergent list_databases still listed.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds SparkSession.* through repark-rooted locals, Builder.* through session/builder locals, and Catalog.* through the catalog local; every example binds each COVERS name through the real receiver.
      artifacts: [scripts/check_example_coverage.py, docs/examples/session/builder.py, docs/examples/session/session_state.py, docs/examples/catalog/set_current_names.py]
    - id: AT-4
      status: N/A
      justification: The gate is a read-only process over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the sixteen local examples and the two pin tests; example children drop AWS_* and PYTHONPATH and run in a fresh temp cwd.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; the red-first provocation ran the AST-only half at the base with exactly 34 findings.
      artifacts: [scripts/check_example_coverage.py]
    - id: AT-8
      status: ATTACKED
      evidence: make ci stays native-build-free with the new examples; the walk adds no import of the facade.
      artifacts: [Makefile, scripts/check_example_coverage.py]
    - id: AT-9
      status: N/A
      justification: Findings print to stderr through the existing reporter; no new log or metric surface.
    - id: AT-10
      status: ATTACKED
      evidence: The pins citation for C-001 lives in task/ledgers/staging/map.md beside the prior example batches, and the pin tests cite the registry rows in their one-line docstrings.
      artifacts: [task/ledgers/staging/map.md, python/repark/tests/test_examples_window_catalog.py, docs/examples/catalog/register_function.py, docs/examples/session/session_state.py]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Pins: [../../../python/repark/tests/test_examples_window_catalog.py](../../../python/repark/tests/test_examples_window_catalog.py)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 `EX-SES-1`..`EX-SES-5` (and `EX-CAT-2` for the kept-back `list_databases`)
- Siblings: [ex-19-dataframe-d-window-ledger.md](ex-19-dataframe-d-window-ledger.md) (the EX-20 window/catalog ledger joins staging with its merge)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-21-catalog-session
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-21-catalog-session
  artifacts_verified:
    ledger: PASS (C-001 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review-gap table carries the eleven in-lane round-1 and round-2 resolutions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, Catalog remainder + SparkSession surface (a) — 34 covered, 1 kept back, 5 divergent arms filed (round 2 re-measured EX-SES-2 and filed EX-SES-3..5)
  verdict: PENDING
  rejection_route: N/A
```
