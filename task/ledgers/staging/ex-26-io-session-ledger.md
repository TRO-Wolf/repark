# Unit ledger — EX-26 · v1.1 example backfill, the reader, writer, session and DataFrame long tail

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands (the
orchestrator's departure move). This file closes when EX-26 merges, or when the owner closes the
slate row.

**Unit:** EX-26 · **Date:** 2026-09-06 · **Model:** muse-spark-1.3 · **Branch:** `docs/ex-26-io-session` · **Base:** `24932dee` (= `origin/main` at dispatch; no merge performed — the orchestrator merges)
**Slate:** [briefs/example-backfill.md](../../../briefs/example-backfill.md), EX-26 lane brief (50 roster names). **Ruling:** owner, 2026-08-31, [release-roadmap-2026-08-29.md](../../roadmap/epic-term/release-roadmap-2026-08-29.md) §"v1.1 — Full example documentation (was v0.7)".

**Rubric:** STANDARD. Floor S1. `risk_tier: standard`.

**Writable paths:** `docs/examples/{io,session,dataframe,catalog}/`, `docs/examples/backlog.txt`,
the `BACKLOG_BASELINE` constant in `scripts/check_example_coverage.py`,
`docs/spark-sql-iceberg-parity.md` §7, `python/repark/tests/test_examples_io_session.py`,
lockstep `map.md` files, and this ledger with its `staging/map.md` row. Closed: `crates/`,
`python/repark/src/`, every other `scripts/` line, `.github/`, `STATUS.md`, every other ledger,
`briefs/next-sequence.md`.

## Scope

The roster is the 50-name io/session/dataframe/catalog enumeration of the lane brief,
re-derived against `docs/examples/backlog.txt` at the base `24932dee` (all 50 present; C-001
records the exact list). The oracle for the Spark-named surface is live PySpark 4.1.2 (ANSI on,
UTC session zone): every asserted value in every example was measured there first (schema AND
rows, plus written-file bytes and layout for the writer names), then matched on repark. The
repark-only names (`smartCsv`, `dynamicFlatten`/`dynamic_flatten`, the writer snake spellings)
assert repark's documented answer and say so in the module docstring's one line. Twenty-nine
names are covered by twelve new scripts; seventeen keep their prior units' stays rows
(EX-DF-1/2/3/4/7/8/17/19, EX-CAT-1, EX-W2-1); the four excel names stay with the new §7 EX-IO-7
row (the engine connector is deferred post-milestone-one, and Spark has no excel reader, so no
oracle values exist). Ten new §7 rows (EX-IO-1..9, EX-SES-6) pin the diverged arms of covered
names, with eleven tests in the new `test_examples_io_session.py`; EX-SES-1's Spark
half-sentence gains a dated correction (EX-26 re-measured `spark.udf.register`'s return twice
plus the installed source). No `catalog/` script ships: both roster catalog names keep their
stays rows.

**Roster (50):** `Catalog.getDatabase`, `Catalog.get_database`, `DataFrame.colRegex`,
`DataFrame.col_regex`, `DataFrame.createGlobalTempView`, `DataFrame.createOrReplaceGlobalTempView`,
`DataFrame.create_global_temp_view`, `DataFrame.describe`, `DataFrame.dynamicFlatten`,
`DataFrame.dynamic_flatten`, `DataFrame.exceptAll`, `DataFrame.except_all`,
`DataFrame.groupingSets`, `DataFrame.grouping_sets`, `DataFrame.intersectAll`,
`DataFrame.intersect_all`, `DataFrame.toJSON`, `DataFrameReader.csv`, `DataFrameReader.excel`,
`DataFrameReader.format`, `DataFrameReader.json`, `DataFrameReader.load`,
`DataFrameReader.option`, `DataFrameReader.options`, `DataFrameReader.schema`,
`DataFrameReader.sheet_names`, `DataFrameReader.smartCsv`, `DataFrameReader.table`,
`DataFrameStatFunctions.freqItems`, `DataFrameWriter.csv`, `DataFrameWriter.format`,
`DataFrameWriter.insertInto`, `DataFrameWriter.insert_into`, `DataFrameWriter.json`,
`DataFrameWriter.option`, `DataFrameWriter.options`, `DataFrameWriter.partitionBy`,
`DataFrameWriter.partition_by`, `DataFrameWriter.save`, `DataFrameWriter.saveAsTable`,
`DataFrameWriter.save_as_table`, `DataFrameWriterV2.overwrite`, `SparkSession.excel_sheet_names`,
`SparkSession.read_excel`, `SparkSession.sparkContext`, `SparkSession.sql`, `SparkSession.table`,
`SparkSession.udf`, `SparkSession.udtf`, `SparkSession.version`.

**Grouping (12 files):**

| File | `COVERS` (roster names) | Why these together |
|---|---|---|
| `io/reader_csv_json.py` | `DataFrameReader.csv`, `DataFrameReader.json`, `DataFrameReader.option`, `DataFrameReader.schema` | The file-reader shorthands with their everyday arms: defaults, header, null value, bare names, explicit schemas. |
| `io/reader_format_load.py` | `DataFrameReader.format`, `DataFrameReader.load`, `DataFrameReader.options`, `DataFrameReader.table` | The builder spelling over all three local formats plus the path option, and table-by-name with its missing-name raise. |
| `io/reader_smart_csv.py` | `DataFrameReader.smartCsv` | The messy-file ingest extension on its own: preamble skip, header detect, inference. |
| `io/writer_csv.py` | `DataFrameWriter.csv`, `DataFrameWriter.format`, `DataFrameWriter.option`, `DataFrameWriter.options`, `DataFrameWriter.save` | The csv write spellings, asserting bytes and data-file counts. |
| `io/writer_json.py` | `DataFrameWriter.json` | The NDJSON write, byte-identical to Spark. |
| `io/writer_partition.py` | `DataFrameWriter.partitionBy`, `DataFrameWriter.partition_by` | The partitioned layout: directory names and per-partition bytes, both spellings. |
| `io/writer_tables.py` | `DataFrameWriter.saveAsTable`, `DataFrameWriter.save_as_table`, `DataFrameWriter.insertInto`, `DataFrameWriter.insert_into` | Table persistence and positional insert, both spellings, with the exists/missing raises. |
| `session/sql_table.py` | `SparkSession.sql`, `SparkSession.table` | SQL reads and table-by-name with the missing-name raise. |
| `session/version_context.py` | `SparkSession.version`, `SparkSession.sparkContext` | Session identity: the version contract and the context fields. |
| `session/udf.py` | `SparkSession.udf` | Scalar-UDF registration with SQL and frame use. |
| `session/udtf.py` | `SparkSession.udtf` | Table-function registration with literal-arg FROM reads. |
| `dataframe/dynamic_flatten.py` | `DataFrame.dynamicFlatten`, `DataFrame.dynamic_flatten` | The native flatten: struct expansion plus list explosion, both spellings. |

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | The 50-name roster above is exactly the lane brief's enumeration, every name re-derived from `docs/examples/backlog.txt` at the base `24932dee`; 29 names are covered by the twelve example units in the grouping table and 21 stay on the backlog (17 with prior stays rows, 4 excel names with EX-IO-7); no `catalog/` script ships. | The backlog grep at dispatch (all 50 present), the shipped examples, and the oracle table (50 rows, one per roster name). | **PROVEN** |
| C-002 | `io/reader_csv_json.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured dtypes and rows for the csv default/header/schema/null-value/bare arms and the json default/schema arms; the infer arm is not taught (EX-IO-3); every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its four names. | **PROVEN** |
| C-003 | `io/reader_format_load.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured answers for `format`/`load` over csv/json/parquet (path option and `options` included) and `table` on a temp view, plus the missing-name `AnalysisException` arm; the bare-load default is not taught (EX-IO-1); every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its four names. | **PROVEN** |
| C-004 | `io/reader_smart_csv.py` runs green under `python <path>` with no network and no JVM, asserts repark's documented columns, dtypes, and rows for the messy-file and explicit-header arms, and says in its one-line docstring that it is a repark extension with no Spark analog. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle row for the name. | **PROVEN** |
| C-005 | `io/writer_csv.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured bytes for the explicit-header csv arms and the `format`/`option`/`options`/`save` spellings; the header default is not taught (EX-IO-4) and neither is the save default (EX-IO-5); every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its five names. | **PROVEN** |
| C-006 | `io/writer_json.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured bytes and the single-data-file count for both the shorthand and the format spellings. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle row for the name. | **PROVEN** |
| C-007 | `io/writer_partition.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured `name=` directory layout and per-partition bytes for `partitionBy`, and the same repark-measured layout for the repark-only `partition_by` spelling. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its two names. | **PROVEN** |
| C-008 | `io/writer_tables.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured rows and dtypes for `saveAsTable` and positional `insertInto` plus the same repark-measured rows for the repark-only snake twins, and the exists/missing `AnalysisException` arms; non-iceberg formats are not taught (EX-IO-6); every `COVERS` name is used in the body. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its four names. | **PROVEN** |
| C-009 | `session/sql_table.py` runs green under `python <path>` with no network and no JVM, asserts the Spark-measured dtypes and rows for a filtered ordered `sql` select and a `table` temp-view read, plus the missing-name `AnalysisException` arm. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its two names. | **PROVEN** |
| C-010 | `session/version_context.py` runs green under `python <path>` with no network and no JVM, asserts the `repark-<dist>` version contract (§8; never parses as a Spark release) and the context arms (builder-echoed master, non-empty application id, accepted `setLogLevel`). | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its two names. | **PROVEN** |
| C-011 | `session/udf.py` runs green under `python <path>` with no network and no JVM, registers a scalar UDF and asserts the Spark-measured SQL and frame values; the register return arm is not taught (EX-SES-6). | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle row for the name. | **PROVEN** |
| C-012 | `session/udtf.py` runs green under `python <path>` with no network and no JVM, registers a table function and asserts the Spark-measured dtypes and rows for a literal-arg `FROM` read. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle row for the name. | **PROVEN** |
| C-013 | `dataframe/dynamic_flatten.py` runs green under `python <path>` with no network and no JVM, asserts repark's documented columns, dtypes, and rows for struct expansion plus list explosion (both spellings agreeing) and the no-explode arm, and says in its one-line docstring that it is a repark extension with no Spark analog. | The shipped script (executed by the `--require-execute` gate leg and standalone) and the oracle rows for its two names. | **PROVEN** |
| C-014 | The 29 covered names leave `docs/examples/backlog.txt` and `BACKLOG_BASELINE` moves down by exactly 29, 193 → 164, with no other `scripts/` change; the backlog delta is exactly the covered set; the gate's static half and its `--require-execute` leg both exit 0 (747 covered; 164 backlog; 202 examples on this tree). | The gate's own counts line at the base `24932dee` (718/193/190, the unit's before-line) and on the shipped tree (747/164/202), plus the red-first provocation below. | **PROVEN** |
| C-015 | Every diverged arm of a covered name carries a §7 row naming its measured cell (EX-IO-1..9, EX-SES-6), the four excel names stay with EX-IO-7, and every new row carries a pin in `python/repark/tests/test_examples_io_session.py` (11 tests, all green); EX-SES-1's Spark half-sentence carries a dated correction that leaves its pin untouched. | The ten registry rows plus the dated correction, the pin file's green run, and the oracle table's stayed column. | **PROVEN** |

`LOGIC_SCORE` = **15/15 `PROVEN`**.

## Red-first (docs/testing.md "Gate provocation proofs")

**Provocation 1 — the backlog ratchet (round 1, on this tree):** the twelve new example files held
outside `docs/examples/` (in gitignored scratch) while `docs/examples/backlog.txt` kept the 29
roster rows deleted and `BACKLOG_BASELINE` stood at 164 (`193 − 29`, as if the whole covered set
were taught): the gate exits **1** with exactly **29 findings**, every one
`public name <name> has no example COVERS row…` naming exactly the 29 covered names, and no other
finding. Restoring the twelve files returns the static gate to **0** (`747 covered; 164 backlog;
202 examples`).
`pins: ex-26-io-session/C-001, C-014`

**Provocation 2 — the execute-leg control (round 1, on this tree):** one provocation injected into
`io/writer_csv.py` (temporary, never committed, reverted before the next run): the asserted
header bytes `['id,name\n1,a\n2,b\n']` overwritten with `['id,name\n1,a\n9,z\n']`. Full gate
`.venv/bin/python scripts/check_example_coverage.py --require-execute`: exit **1** with exactly
one execute finding, `example …/docs/examples/io/writer_csv.py exited 1: … csv header bytes
['id,name\n1,a\n2,b\n'] != ['id,name\n1,a\n9,z\n']` — the control names the script and both
values. Reverting restores the full gate to **0** and the tree to clean.
`pins: ex-26-io-session/C-005, C-014`

## Oracle (live PySpark 4.1.2, ANSI on, UTC, 2026-09-06)

The measurement instrument is four shared probe scripts (`scratch/ex26/spark_probe_reads.py`,
`spark_probe_writes.py`, `repark_probe_reads.py`, `repark_probe_writes.py`, gitignored) run once
per engine with identical cells, plus a second round (`spark_probe_round2.py`,
`repark_probe_round2.py`, `repark_probe_bytes.py`) for the save defaults, the `options()` plural,
the file bytes, the hasattr batch, and the repark-only cells. The Spark halves below are verbatim
cell output (banner `4.1.2` / `UTC` on every JVM run). Each shipped example was also run
standalone from a temp cwd before any commit (exit 0, all twelve). The lane venv resolves
`repark` to the lane with a release native (`repark._native.__debug_assertions__` False, built by
`maturin develop --release` in-lane). Three JVM runs, each stopped by its script
(`spark.stop()`); no lane JVM remained afterwards. The seventeen pre-disposed stays were not
re-measured: their rows and pins stand, and the pin suites re-run green in the gates table.

| Name | Spark cell | repark cell | Verdict | File / row |
|---|---|---|---|---|
| `Catalog.getDatabase` | `('default', 'spark_catalog', 'default database', 'file:<wh>')` | `description=None, locationUri=None` | stayed (prior) | EX-CAT-1 |
| `Catalog.get_database` | same twin | same twin | stayed (prior) | EX-CAT-1 |
| `DataFrame.colRegex` | backticked spelling selects; plain raises | plain selects; backticked raises; first match only | stayed (prior) | EX-DF-1 |
| `DataFrame.col_regex` | same twin | same twin | stayed (prior) | EX-DF-1 |
| `DataFrame.createGlobalTempView` | registers `global_temp.gt` | refuses, no global_temp catalog | stayed (prior) | EX-DF-2 |
| `DataFrame.createOrReplaceGlobalTempView` | swaps the definition | refuses | stayed (prior) | EX-DF-2 |
| `DataFrame.create_global_temp_view` | n/a (repark spelling of the same refusal) | refuses | stayed (prior) | EX-DF-2 |
| `DataFrame.describe` | stable count/mean/stddev/min/max order | same cells, engine-arbitrary order | stayed (prior) | EX-DF-4 |
| `DataFrame.dynamicFlatten` | no such name (`hasattr` False) | flattens struct, explodes lists | covered (repark-only) | `dataframe/dynamic_flatten.py` |
| `DataFrame.dynamic_flatten` | no such name (`hasattr` False) | agrees with the camel twin | covered (repark-only) | `dataframe/dynamic_flatten.py` |
| `DataFrame.exceptAll` | `[(1,), (2,)]` multiset difference | refuses | stayed (prior) | EX-DF-3 |
| `DataFrame.except_all` | same (Spark spells `exceptAll`) | refuses | stayed (prior) | EX-DF-3 |
| `DataFrame.groupingSets` | list-of-sets signature, 6 rows | one-column-each, different rows | stayed (prior) | EX-DF-8 |
| `DataFrame.grouping_sets` | same twin | same twin | stayed (prior) | EX-DF-8 |
| `DataFrame.intersectAll` | `[(1,), (1,)]` multiset intersect | refuses | stayed (prior) | EX-DF-7 |
| `DataFrame.intersect_all` | same (Spark spells `intersectAll`) | refuses | stayed (prior) | EX-DF-7 |
| `DataFrame.toJSON` | one JSON string per row | refuses | stayed (prior) | EX-DF-17 |
| `DataFrameReader.csv` | default/header/schema/nullValue/bare arms | identical dtypes+rows; infer width differs | covered + BACKLOG ARM | `io/reader_csv_json.py` + EX-IO-3 |
| `DataFrameReader.excel` | no such name (`hasattr` False) | refuses, connector deferred | stayed | EX-IO-7 |
| `DataFrameReader.format` | builder over csv/json/parquet | identical | covered | `io/reader_format_load.py` |
| `DataFrameReader.json` | `bigint` id + rows; schema arm | identical | covered | `io/reader_csv_json.py` |
| `DataFrameReader.load` | format loads + parquet default | loads identical; bare default raises | covered + BACKLOG ARM | `io/reader_format_load.py` + EX-IO-1 |
| `DataFrameReader.option` | header/path arms | identical | covered | `io/reader_csv_json.py` |
| `DataFrameReader.options` | plural equals singular | identical | covered | `io/reader_format_load.py` |
| `DataFrameReader.schema` | csv/json/parquet arms | csv/json identical; parquet refuses | covered + BACKLOG ARM | `io/reader_csv_json.py` + EX-IO-2 |
| `DataFrameReader.sheet_names` | no such name (`hasattr` False) | refuses, connector deferred | stayed | EX-IO-7 |
| `DataFrameReader.smartCsv` | no such name (`hasattr` False) | preamble skip, header detect, `int` infer | covered (repark-only) | `io/reader_smart_csv.py` |
| `DataFrameReader.table` | temp-view rows; missing `TABLE_OR_VIEW_NOT_FOUND` | rows identical; missing text differs | covered + BACKLOG TEXT | `io/reader_format_load.py` + EX-IO-8 |
| `DataFrameStatFunctions.freqItems` | frequent-item table | refuses | stayed (prior) | EX-DF-19 |
| `DataFrameWriter.csv` | header-false default; explicit arms | explicit arms byte-identical; default headers | covered + BACKLOG ARM | `io/writer_csv.py` + EX-IO-4 |
| `DataFrameWriter.format` | csv/json + save spellings | identical | covered | `io/writer_csv.py` |
| `DataFrameWriter.insertInto` | positional `[(1,a),(2,b),(9,z)]` | identical; missing text differs | covered + BACKLOG TEXT | `io/writer_tables.py` + EX-IO-8 |
| `DataFrameWriter.insert_into` | no such name (`hasattr` False) | same rows as the camel twin | covered (repark-only) | `io/writer_tables.py` |
| `DataFrameWriter.json` | `{"id":1,"name":"a"}` lines | byte-identical | covered | `io/writer_json.py` |
| `DataFrameWriter.option` | header/sep arms | identical | covered | `io/writer_csv.py` |
| `DataFrameWriter.options` | plural equals singular | identical | covered | `io/writer_csv.py` |
| `DataFrameWriter.partitionBy` | `name=` dirs, `1`/`2` bytes | identical dirs + bytes | covered | `io/writer_partition.py` |
| `DataFrameWriter.partition_by` | no such name (`hasattr` False) | identical to the camel twin | covered (repark-only) | `io/writer_partition.py` |
| `DataFrameWriter.save` | explicit formats; parquet default | explicit identical; default raises | covered + BACKLOG ARM | `io/writer_csv.py` + EX-IO-5 |
| `DataFrameWriter.saveAsTable` | rows+dtypes; csv format served | rows+dtypes identical; csv refuses | covered + BACKLOG ARM | `io/writer_tables.py` + EX-IO-6 |
| `DataFrameWriter.save_as_table` | no such name (`hasattr` False) | same rows as the camel twin | covered (repark-only) | `io/writer_tables.py` |
| `DataFrameWriterV2.overwrite` | conditional overwrite `[(1,'aa'),(2,'b')]` | refuses | stayed (prior) | EX-W2-1 |
| `SparkSession.excel_sheet_names` | no such name (`hasattr` False) | refuses, connector deferred | stayed | EX-IO-7 |
| `SparkSession.read_excel` | no such name (`hasattr` False) | refuses, connector deferred | stayed | EX-IO-7 |
| `SparkSession.sparkContext` | `local[1]`, app name, `local-<ts>` id | master echoes builder; stable id; `setLogLevel` None | covered (shape-check) | `session/version_context.py` |
| `SparkSession.sql` | filtered ordered select, `int`/`double` | identical dtypes+rows | covered | `session/sql_table.py` |
| `SparkSession.table` | temp-view rows; missing `TABLE_OR_VIEW_NOT_FOUND` | rows identical; missing text differs | covered + BACKLOG TEXT | `session/sql_table.py` + EX-IO-8 |
| `SparkSession.udf` | register + `u4`/`u1`/`u2` values | values identical; return arm differs | covered + BACKLOG ARM | `session/udf.py` + EX-SES-6 |
| `SparkSession.udtf` | `FROM plus26(1)` → `[(1, 2)]` | identical dtypes+rows | covered | `session/udtf.py` |
| `SparkSession.version` | `'4.1.2'` | `'repark-1.0.1'` (§8 contract) | covered (shape-check) | `session/version_context.py` |

## Gates (2026-09-06, on this tree)

| Command | Exit |
|---|---|
| `make check-example-coverage` | **0** (`747 covered; 164 backlog; 2 exceptions; 202 examples`; static half, system python3) |
| `.venv/bin/python scripts/check_example_coverage.py --require-execute` | **0** (same counts; all twelve example units executed) |
| `make check-python-conventions` | **0** (253 files clean) |
| `make py-lint` | **0** |
| `make py-format-check` | **0** (751 files formatted) |
| `.venv/bin/python -m pytest python/repark/tests/test_examples_*.py -q` | **0** (66 passed: 55 existing + 11 new) |
| `for f in` the twelve new scripts `; do .venv/bin/python $f; done` (each from a temp cwd) | **0** (all twelve PASS) |
| `make check-map-sync` | **0** |
| `make check-ledger-grammar` | **0** |
| `make check-ledgers` | **0** |
| `make check-docs-compaction` | **0** |
| `python3 scripts/ledger_lifecycle.py check --base origin/main` | **0** |
| `typos .` | **0** |

Counts line (on this tree; the base `24932dee` run printed `718 covered; 193 backlog;
2 exceptions; 190 examples`):

`example-coverage: 913 public names (catalog=28, column=40, dataframe=150, functions=444, io=42, ml=28, session=41, ta=86, types=32, window=22); 747 covered; 164 backlog; 2 exceptions; 202 examples`

Before this unit: `718 covered; 193 backlog; 190 examples` (at `24932dee`, `BACKLOG_BASELINE`
193). On this unit's tree: `747 covered; 164 backlog; 202 examples` (`BACKLOG_BASELINE` 193 →
164) — exactly the 29 covered roster names, +29 / −29 / +12.

## Review notes (round 1, in-lane)

| Finding | Disposition |
|---|---|
| The lane brief's Deliverable section describes the EX-25 functions grouping (`array_more.py`, `strings_more.py`, …) verbatim | stale copy from the EX-25 brief: those files already ship. The Roster section, the branch name (`docs/ex-26-io-session`), and the writable paths (`docs/examples/{io,session,dataframe,catalog}/`) govern instead — recorded here and in the handback |
| The brief says "≈46 names" but enumerates 50; the `ml.*` backlog (28 names) is neither enumerated nor writable | the enumeration is the roster (50, all present at dispatch); `ml.*` stays a later unit's family — recorded in C-001 |
| The first commit attempt was rejected by the map-lockstep hook (family maps missing) | maps added and committed with the scripts; no amend |
| `ruff format` reflowed 5 of the 12 files after the first lint pass | reformatted and all twelve re-run green before any commit; `ruff format --check` exits 0 on the shipped tree |
| repark writes no `_SUCCESS`/`.crc` sidecars and its data-file names are random (`LRx1…_0.csv`), where Spark writes `part-00000-<uuid>-c000.<ext>` plus sidecars | disclosed, not rowed: sidecars and exact names are Hadoop/filesystem artifacts, not the writer contract. The examples assert data-file counts, suffixes, and bytes only |
| `save()` without a path: Spark raises `IllegalArgumentException`, repark `AnalysisException`, with the identical text `'path' is not specified.` | observed, not rowed: a programming-misuse arm where both engines raise with the same text; error-contract-only |
| `csv()` without a path: Spark raises `TypeError` (missing arg), repark `AnalysisException`, and repark additionally accepts `option("path", …)` | observed, not rowed: same class as above, plus a repark superset spelling (not a divergence) |
| Unknown `format()` on `load()`: Spark raises lazily at collect (`Py4JJavaError` wrapping `DATA_SOURCE_NOT_FOUND`), repark eagerly at `load()` with near-identical text | observed, not rowed: eagerness of an error both engines raise |
| EX-SES-1's Spark half-sentence ("`spark.udf.register` answers the `UserDefinedFunction`") contradicts this unit's two JVM measurements plus the installed source (`register` returns `udf_obj._wrapped()`, a plain `function`) | dated correction applied to the row's Spark bullet; its pin (repark's `catalog.registerFunction` return) is untouched. The new EX-SES-6 row carries the correct cells |

## Cost

The Muse Spark (muse-spark-1.3) leg started 2026-09-06: read the contract, the slate, the
corpus (the EX-25/EX-24 ledgers, the io/session/dataframe/catalog maps, the gate, the §7
`EX-*` rows), and the facade for the roster surface (reader, writer, session, `dynamicFlatten`,
`colRegex`, the UDF/UDTF registries); built the lane release native (`maturin develop
--release`) and installed the pinned PySpark 4.1.2 oracle into the lane venv; measured all 33
undecided names on both engines through shared probe scripts (three JVM runs, each stopped by
its script); wrote the twelve example files, the eleven pins, the ten §7 rows plus the EX-SES-1
correction, and this ledger, ran both red-first provocations, then the ratchet, the maps, and
the full gate list, committing in slices. Base `24932dee`.

## Disk

Pickup: `df -h` 945 GB free of 1.8 TB. The provocation scratch lives under the gitignored
`scratch/ex26/` (probe scripts, held-aside example copies, captured Spark/repark cell logs —
132 KB, all removable at close). Lane-local weight added by this unit: `.ivy2/` (182 MB, copied
from `~/.ivy2.5.2` per the brief's ivy redirect; untracked), the PySpark 4.1.2 install in the
lane venv, and the release native build under `target/` (2.1 GB, untracked build cache, shared
with any later lane build). No worktree created (lane-local unit). No build artifacts committed.

## Dual-wire

Unchanged by this unit. Static half: `make check-example-coverage` and ci.yml python job
(`./scripts/check_example_coverage.sh`). Execute half: wheels.yml smoke
`python -I scripts/check_example_coverage.py --require-execute` after the packaged wheel is
installed. EX-26 moves only the inventory/backlog ratchet, example files, §7 rows, and pins;
it moves no wire, and `.github/` is closed to this unit.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ex-26-io-session
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The AST walk emits 913 names across ten families; the 29 covered roster names are taught by twelve new example files, and the oracle table records the Spark cell, the repark cell, and the verdict for all 50 rows.
      artifacts: [scripts/check_example_coverage.py, docs/examples/inventory.txt, docs/examples/io/reader_csv_json.py, docs/examples/io/reader_format_load.py, docs/examples/io/reader_smart_csv.py, docs/examples/io/writer_csv.py, docs/examples/io/writer_json.py, docs/examples/io/writer_partition.py, docs/examples/io/writer_tables.py, docs/examples/session/sql_table.py, docs/examples/session/version_context.py, docs/examples/session/udf.py, docs/examples/session/udtf.py, docs/examples/dataframe/dynamic_flatten.py]
    - id: AT-2
      status: ATTACKED
      evidence: Red-first provocation 1 held the twelve files outside docs/examples while the backlog rows stayed deleted and the baseline stood at 164; the gate exited 1 with exactly 29 findings and the backlog is an exact baseline 164.
      artifacts: [scripts/check_example_coverage.py, docs/examples/backlog.txt]
    - id: AT-3
      status: ATTACKED
      evidence: The gate's use-check binds every class-surface COVERS name on a repark-rooted local; the examples call each covered name on session/frame/reader/writer locals, so a dropped call is an unused-cover red.
      artifacts: [scripts/check_example_coverage.py, docs/examples/io/reader_csv_json.py, docs/examples/io/writer_tables.py, docs/examples/session/udf.py]
    - id: AT-4
      status: N/A
      justification: The gate and the examples are read-only processes over source files and example scripts; no shared mutable engine state.
    - id: AT-5
      status: N/A
      justification: No new execution surface beyond the twelve local examples; they build local frames and files (plus bare-session Iceberg tables for saveAsTable/insertInto), drop AWS_* and PYTHONPATH in the gate's child, and touch no network or cloud service. The Spark JVM ran only the gitignored oracle probes, never the examples.
    - id: AT-6
      status: N/A
      justification: No engine or python/repark/src product change; the backfill walks public names that already exist.
    - id: AT-7
      status: ATTACKED
      evidence: The static gate is AST-only; example execution is skipped when the native module is absent and required when --require-execute is passed; provocation 2 ran the execute half with a wrong-bytes control and it failed by name with exit 1.
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
      evidence: The pins citations for C-001..C-015 live in scripts/map.md beside the prior example batches, the family maps cite their script clauses, the tests map cites C-015, and this ledger cites its clauses in the red-first and oracle sections.
      artifacts: [scripts/map.md, docs/examples/io/map.md, task/ledgers/staging/map.md]
  reattested: []
  complete: true
```

## Pointers

- Up: [map.md](../staging/map.md)
- Slate: [../../../briefs/example-backfill.md](../../../briefs/example-backfill.md)
- Gate: [../../../scripts/check_example_coverage.py](../../../scripts/check_example_coverage.py)
- Oracle: live PySpark 4.1.2 (ANSI on, UTC); probes under gitignored `scratch/ex26/`
- Pins: [../../../python/repark/tests/test_examples_io_session.py](../../../python/repark/tests/test_examples_io_session.py) (11 tests for EX-IO-1..9 and EX-SES-6)
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) §7 (EX-IO-1..9 and EX-SES-6 new; EX-SES-1 corrected)
- Siblings: [ex-25-functions-a-ledger.md](ex-25-functions-a-ledger.md), [ex-24-ta-b-ledger.md](ex-24-ta-b-ledger.md)

```yaml
SHIPPED_FLAG_REGISTER:
  pr_unit: ex-26-io-session
  flags: []
  count: 0

DELIVERY_SIGNOFF:
  pr_unit: ex-26-io-session
  artifacts_verified:
    ledger: PASS (C-001..C-015 PROVEN)
    coverage_attestation: PASS (AT-1..AT-10, complete true)
    findings_ledger: PASS (review notes carry the in-lane round-1 dispositions)
    shipped_flag_register: PASS (count 0)
  done_gate: PASS (gates table)
  status_update: v1.1 example backfill, reader/writer/session long tail — 29 covered, 21 stayed with EX-IO-1..9 and EX-SES-6
  verdict: PENDING
  rejection_route: N/A
```
