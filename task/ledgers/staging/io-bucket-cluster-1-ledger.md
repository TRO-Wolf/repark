# Charter ledger — IO-BUCKET-CLUSTER-1 · `bucketBy sortBy clusterBy writeTo().clusterBy`

**Date:** 2026-09-14 · **Branch:** `feat/io-bucket-cluster-1` · **Base:** `origin/main`
`8cd6d1e6` · **Model:** zai/glm-5.3-flash · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** two rows filed DECLARED — `IO-BUCKET-1` (Ruling R-1) and `IO-CLUSTER-1`
(Ruling R-2) — in docs/spark-sql-iceberg-parity.md §5 (2026-09-14).

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The 1.5 PySpark-parity campaign: every public name of PySpark 4.1.2's writer
surfaces is present on the facade and answers Spark, or carries a dated declared refusal with
Spark's own error class. The run-15b oracle recorded fourteen `io.*` writer-layout cells on live
PySpark 4.1.2; this unit binds `DataFrameWriter.bucketBy` / `sortBy` / `clusterBy` and
`DataFrameWriterV2.clusterBy` and drives every cell.

**Not in this unit:** Hive bucket file writing (R-1: Iceberg has none — the refusal IS the
parity answer), Iceberg clustering-column recording (R-2), `insertInto` × `bucketBy` (Spark
refuses there per its `assertNotBucketed("insertInto")`; the card's table does not settle that
shape and it is filed as a question, not guessed), and the `text_*`/`xml_*`/`orc_*`/`jdbc_*`/
`na_replace_*` cells of the same oracle fixture (other runs own them).

## PROPOSITION LEDGER — IO-BUCKET-CLUSTER-1 — 2026-09-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DataFrameWriter.bucketBy(numBuckets, col, *cols)` / `sortBy(col, *cols)` answer the seven measured `bucketBy_*` / `sortBy_*` cells: `NOT_INT` on a non-int `numBuckets` at the call (message and params byte-equal; extra `None`/`float` shapes same class), a list first column accepted and both spellings returning the writer, every path save (`save`/`parquet`/`csv`/`json`/`format("text")`) with bucketing raising `_LEGACY_ERROR_TEMP_1312` `'save' does not support bucketBy right now.`, `sortBy` without `bucketBy` raising `SORT_BY_WITHOUT_BUCKETING` (cell measured at `saveAsTable`; the card's rule is stated without a door, so the path door raises it too), `numBuckets` 0/−1/100001 raising `INVALID_BUCKET_COUNT` at the save, a bucket column absent from the frame raising `COLUMN_NOT_DEFINED_IN_TABLE` with the backticked multipart name (repark's default `spark_catalog.default` resolution matches the oracle's verbatim), and a valid bucketed `saveAsTable` refusing `NOT_IMPLEMENTED` under Ruling R-1 without creating the table. | `test_io_bucket_cluster_1.py::test_bucketBy_*` + `test_sortBy_without_bucketBy_refused`, the `io.bucketBy_*` / `io.sortBy_*` cells. | **PROVEN** | All seven cells green. Red first on base `8cd6d1e6`: 13 of the file's 14 pins failed (`AttributeError: 'DataFrameWriter' object has no attribute 'bucketBy'` and the sibling names), 1 passed (the C-004 guard was green on base). `bucketBy_zero`'s message asserts byte-equal including `SQLSTATE: 22003`; `bucketBy_missing_col` byte-equal including `SQLSTATE: 42703` and the not-created table. Structured classes carry Spark's errorClass/params/SQLSTATE via the `_integral` attach helpers, the df-stream-batch-1 pattern. pins: io-bucket-cluster-1/C-001 |
| C-002 | `DataFrameWriter.clusterBy(*cols)` and `DataFrameWriterV2.clusterBy(col, *cols)` answer the five measured `clusterBy_*` / `v2_clusterBy_*` cells: both spellings chain and return their writer, a path save ignores clustering and writes (`clusterBy_save` answered `None`), `clusterBy` with `partitionBy` or `bucketBy` refuses at the save in both call orders (`SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED` / `SPECIFY_CLUSTER_BY_WITH_BUCKETING_IS_NOT_ALLOWED`), V2 refuses the `partitionedBy` conflict at `create`/`replace`/`createOrReplace` (`v2_clusterBy_partitionedBy`, checked before table resolution), V2 `create`/`createOrReplace` refuse `NOT_IMPLEMENTED` `{"feature": "clusterBy on an Iceberg table"}` under Ruling R-2 (where Spark answered `None` by writing a non-Iceberg session-catalog table), and V1 `saveAsTable` refuses the same way (`clusterBy_saveAsTable` — Spark records `# Clustering Information`). | `test_io_bucket_cluster_1.py::test_clusterBy_*` + `test_v2_clusterBy_*`. | **PROVEN** | All five cells green on the same red-first run. The refusals fire before any table is created (`tableExists` False asserted). The conflict messages assert byte-equal including `SQLSTATE: 42908`. pins: io-bucket-cluster-1/C-002 |
| C-003 | The unit's docs deliverables land whole: the run-15b fixture copied unchanged into `python/repark/tests/` with a map row, the new example `docs/examples/io/writer_bucket_cluster.py` covering all eight new inventory names (both spellings, both writers, refusal arms), `inventory.txt` refreshed by `--write-inventory`, the `test_ex_0` raw-walk count moved 936 → 944, the registry rows `IO-BUCKET-1` / `IO-CLUSTER-1` at §5 end, and every touched directory's `map.md` in lockstep. | `scripts/check_example_coverage.py` (and `--write-inventory`), `test_ex_0_example_coverage.py`, `scripts/check_lib_py.py` + the CAP-1 mirror, `make check-map-sync`. | **PROVEN** | Fixture copied at 25,064 bytes (47 cells, provenance line inside). Example gate green with execution (219 examples, native module present): 937 example-inventory names (io=50), 823 covered, backlog 112 unchanged, exceptions 2 unchanged; raw walk 936 + 8 = 944 and the ex_0 pin moved with it (26 passed). `writer_readwriter.py` 1111 → 1105 ratcheted DOWN in `check_lib_py.py` and the CAP-1 duplicate table (never raised); `writer_layout.py` (312 lines) is below the default. Maps: dataframe, tests, examples/io, ledgers/staging. pins: io-bucket-cluster-1/C-003 |
| C-004 | No regression on the touched surface: the existing writer suites, the e2 readwriter suite, the API freeze inventory, and the size/example gates stay green with the new bindings, slots, moved helpers, and action-time checks. | `test_writer.py`, `test_writer_v2.py`, `test_e2_readwriter.py`, `test_api_freeze.py`, `test_cap_1_source_file_line_cap.py`, `check_lib_py.py`. | **PROVEN** | `test_io_bucket_cluster_1.py + test_writer.py + test_writer_v2.py + test_e2_readwriter.py` 109 passed. The v1 `saveAsTable` path gained five check calls ahead of the existing flow and the V2 actions three; `test_partitioned_saveAsTable_still_writes` (C-004 guard, green on base too) re-pins the partitioned CTAS + partition-filtered read. API freeze: the packet rows check listed frozen names still exist with their recorded signatures — adding names and moving the helpers behind re-imports drifts nothing. The helper move keeps `core.py`'s import surface byte-identical (`from repark.spark.dataframe.writer_readwriter import _merge_path_write_tree, _normalize_*, _sql_option_escape` still resolves). pins: io-bucket-cluster-1/C-004 |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Per-name decision table

| Name | Decision | Reason (one line) |
|---|---|---|
| `DataFrameWriter.bucketBy` | implemented | Argument checks and layout state; every table write refuses under R-1 (registry `IO-BUCKET-1`) — Python is correct here: API plumbing, no engine capability approximated. |
| `DataFrameWriter.bucket_by` | implemented | House snake alias of `bucketBy`, covered by the same example and pins. |
| `DataFrameWriter.sortBy` | implemented | Argument checks only; `SORT_BY_WITHOUT_BUCKETING` at the action — API plumbing. |
| `DataFrameWriter.sort_by` | implemented | House snake alias of `sortBy`. |
| `DataFrameWriter.clusterBy` | implemented | Layout state; table writes refuse under R-2 (registry `IO-CLUSTER-1`), path saves write as Spark does — API plumbing. |
| `DataFrameWriter.cluster_by` | implemented | House snake alias of `clusterBy`. |
| `DataFrameWriterV2.clusterBy` | implemented | Layout state; `create`/`replace`/`createOrReplace` check conflicts then refuse under R-2 — API plumbing. |
| `DataFrameWriterV2.cluster_by` | implemented | House snake alias of V2 `clusterBy`. |
| Hive bucketing on `saveAsTable` | **DECLARED** | Ruling R-1, row `IO-BUCKET-1` (2026-09-14): Iceberg has no Hive bucketing; Spark writes Hive bucket files (cell `bucketBy_saveAsTable`). |
| Iceberg clustering-column recording | **DECLARED** | Ruling R-2, row `IO-CLUSTER-1` (2026-09-14): Spark records clustering columns in the table (cells `clusterBy_saveAsTable`, `v2_clusterBy_create_session_catalog`). |

## 6. What changed

| File | Change |
|---|---|
| `python/repark/src/repark/spark/dataframe/writer_layout.py` | New, 312 lines: the layout setters, the action-time checks with Spark's structured error classes, the two declared refusals, and the five write helpers moved from `writer_readwriter.py`. |
| `python/repark/src/repark/spark/dataframe/writer_readwriter.py` | Thin `bucketBy`/`sortBy`/`clusterBy` (v1) and `clusterBy` (V2) bindings plus snake aliases; layout slots on both writers; the check calls at `save` / `saveAsTable` / V2 `create` / `replace` / `createOrReplace`; the five write helpers re-imported from `writer_layout` so `core.py`'s import surface is unchanged; 1111 → 1105 lines. |
| `python/repark-parity/tests/test_cap_1_source_file_line_cap.py` | The `writer_readwriter.py` duplicate baseline ratchets DOWN 1111 → 1105. |
| `scripts/check_lib_py.py` | The `writer_readwriter.py` exception baseline ratchets DOWN 1111 → 1105. |
| `python/repark/tests/test_io_bucket_cluster_1.py` | New. The fourteen oracle cells plus the extra argument shapes, both spellings, and the C-004 regression guard. |
| `python/repark/tests/facade_reader_writer_oracle.json` | The run-15b oracle fixture copied unchanged. |
| `docs/examples/io/writer_bucket_cluster.py` | New example covering the eight new inventory names. |
| `docs/examples/inventory.txt` | Refreshed by `--write-inventory` (+8 rows). |
| `python/repark-parity/tests/test_ex_0_example_coverage.py` | The raw-walk count pin 936 → 944. |
| `docs/spark-sql-iceberg-parity.md` | Rows `IO-BUCKET-1` and `IO-CLUSTER-1` filed DECLARED at §5 end. |
| `map.md` x 4 | Lockstep: dataframe, tests, examples/io, ledgers/staging. |

No dependency change: `Cargo.toml`, `Cargo.lock`, `pyproject.toml`, `uv.lock`, and `.github/`
are untouched; no Rust file is touched (this clone is Python-only; the native module is the
prebuilt release one).

## 7. Unmeasured-shape decisions (recorded, not guessed)

| Shape | Decision | Basis |
|---|---|---|
| Path save with `sortBy` and no `bucketBy` | Raises `SORT_BY_WITHOUT_BUCKETING` (same as the table door). | The card states the rule without a door ("`sortBy` without `bucketBy` → `SORT_BY_WITHOUT_BUCKETING`"); the door qualifier appears in the card only where it meant one (`bucketBy_zero`, "at save"). A silent ignore would violate the no-silent-stub rule. |
| `numBuckets` bounds check placement | At the action, after the conflict checks, before `COLUMN_NOT_DEFINED_IN_TABLE`. | The card's clause order ("`numBuckets` <= 0 or > 100000 → `INVALID_BUCKET_COUNT` (`bucketBy_zero`, at save); `saveAsTable` with bucketBy: a bucket column missing → …; otherwise Ruling R-1"). |
| V2 conflict checks before table resolution | The `SPECIFY_CLUSTER_BY_*` error fires ahead of `TABLE_OR_VIEW_*`. | The card places the check "at create/replace/createOrReplace"; the probes' targets did not exist, so the measured cells cannot distinguish, and the argument-conflict check is not a table-state check. |
| `insertInto` with `bucketBy` configured | Unchanged by this unit; filed as a handback question. | Spark's v1 writer refuses there (`assertNotBucketed("insertInto")`), but the card's table does not settle that shape and widening the card was not this unit's call. |

## 8. Gates

| gate | result |
|---|---|
| `.venv/bin/python -m pytest python/repark/tests/test_io_bucket_cluster_1.py -q` | 14 passed (13 red on base `8cd6d1e6` before the implementation) |
| `.venv/bin/python -m pytest python/repark/tests/test_io_bucket_cluster_1.py python/repark/tests/test_writer.py python/repark/tests/test_writer_v2.py python/repark/tests/test_e2_readwriter.py -q` | 109 passed |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests/test_ex_0_example_coverage.py -q` | 26 passed |
| `python3 scripts/check_example_coverage.py` (+ `--write-inventory` first) | exit 0; with `.venv/bin/python` the examples execute (219 examples) |
| `python3 scripts/check_lib_py.py` | 694 files clean; `writer_readwriter.py` at its new exact baseline 1105 |
| `uvx ruff@0.15.22 check .` / `format --check .` | clean |
| `PYTHONPATH=python/repark-parity/src .venv/bin/python -m pytest python/repark-parity/tests -q -k "registry or ex_0 or cap_1 or map"` | run at unit close (see handback) |
| `python3 scripts/check_ledger_grammar.py` | only the missing-orchestrator COVERAGE_ATTESTATION finding may remain |
| `typos` on changed files, commit-hook run | clean |
| fence (`git diff --cached … \| grep -P '^\+\s*(//\|#(?!\[\|!\[\| noqa))'`) | empty before every commit |
