# Charter ledger — DF-METADATA-COL-1 · `DataFrame.metadataColumn` and the hidden `_metadata` column

**Date:** 2026-09-16 · **Branch:** `feat/df-metadata-col-1` · **Base:** `02abfd0e`
· **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-METADATA-COL-1` (row lands in step 5) plus BACKLOG `SQL-METADATA-COL-1`.
**Ownership:** run 18b lane. No edits to `functions*.py`, the Rust function
registry, `function_dispatch.rs`, or `crates/repark-spark` (M-5 needs none — see C-005).

**Why now.** The last missing DataFrame census name (114/115 on main). Measured on main:
repark has no `_metadata` at all — `select("_metadata")`, `select("_metadata.file_path")`
and SQL `SELECT _metadata.file_name FROM <view>` all raise "cannot be resolved". Real Rust
feature: a hidden per-file metadata struct on the file scans, plus the facade name.

**Oracle.** 48 live PySpark 4.1.2 cells (ANSI on, UTC) in
[../../../python/repark/tests/facade_df_metadata_col_oracle.json](../../../python/repark/tests/facade_df_metadata_col_oracle.json):
34 from `/tmp/oc-worker/run18b/oracle/metadata_spark_2026-09-16.json` (probe
`probe_metadata.py`, `spark`/`repark` modes) plus the 14 `metadata_*` cells from the
run-16b `dfrust3_probe_2026-09-15.json` recording. Values verified identical after merge.
`{ROOT}` already substituted by the probes. No hand-computed expectation anywhere.

## PROPOSITION LEDGER — DF-METADATA-COL-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| M-1 | `_metadata` is a non-nullable struct: parquet (+partitioned) 7 fields `file_path string, file_name string, file_size bigint, file_block_start bigint, file_block_length bigint, file_modification_time timestamp, row_index bigint`; csv/json/text the same 6 without `row_index`; every field non-nullable; `schema.json()` carries `{"__file_source_metadata_col":true,"__metadata_col":"_metadata"}` on `_metadata`. | `test_df_metadata_col_1.py` schema/json pins over `schema_*`, `json_*`, `mtime_type`; Rust UDF `return_field_from_args` test. | **OPEN** | Before-table §1: all schema/json cells red (`_metadata` unresolvable / no `metadataColumn`). |
| M-2 | `select("*")` / `SELECT *` / `.columns` never show it; reachable by name via `select("_metadata")`, `select("_metadata.field")`, `F.col("_metadata.x")`, and after a projection that dropped it. | `metadata_select_star_hidden`, `names_*`, `sql_star_hidden`, `metadata_col_after_select`, `metadata_col_after_select_named`, `metadata_string_select` pins. | **OPEN** | `sql_star_hidden` and shadow-user-column already green on base; the rest red (§1). |
| M-3 | Values: `file_path` is the `file:`-scheme URI of the part file; `file_name` its base; `file_size` bytes; `file_block_start` 0 and `file_block_length` the file length unsplit; `row_index` 0-based within the file; `file_modification_time` a timestamp (type + sanity bound). | `values_parquet`, `values_csv`, `row_index_parquet`, `metadata_parquet_field`, `metadata_after_filter` pins; Rust per-file mechanism test. | **OPEN** | Red on base (§1). `row_index` is per-file emission order behind one partition per file — never a global row number (R-1). |
| M-4 | `df.metadataColumn(name)`: repr `Column<'_metadata'>`; non-str → `PySparkTypeError` `NOT_STR`; non-file plan → `AnalysisException` `UNRESOLVED_COLUMN.WITHOUT_SUGGESTION`; unknown name → `UNRESOLVED_COLUMN.WITH_SUGGESTION` proposing `_metadata`; across join/aggregate → `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT`; user `_metadata` column shadows (hidden becomes `__metadata`, `metadataColumn` then answers `MISSING_ATTRIBUTES`). | `metadata_col_repr`, `metadata_arg_type`, `metadata_arg_none`, `metadata_on_local`, `metadata_on_range`, `metadata_on_sql_values`, `metadata_missing`, `metadata_missing_lazy`, `metadata_regular_col`, `metadata_bad_name_select`, `metadata_repr_name`, `metadata_col_after_join`, `metadata_col_after_agg`, `metadata_user_column_shadow`, `metadata_user_column_shadow_mc` pins. | **OPEN** | Eager-at-call validation fits every timing cell (R-2). `getCondition()` populated via the errors.py attr-aware patch (R-3). |
| M-5 | SQL door: `SELECT i, _metadata.row_index FROM parquet.\`<path>\`` stays `xfail(strict=True)` against `sql_path_metadata` (the path-table door does not exist on main — `table 'datafusion.parquet.…' not found` — and building it is run 18c's planner); `sql_view_metadata` / `sql_local_metadata` pin today's `AnalysisException` shape green; `SELECT *` over a view stays hidden. BACKLOG row `SQL-METADATA-COL-1` filed. | `test_df_metadata_col_1.py` SQL pins; registry BACKLOG row. | **OPEN** | No `crates/repark-spark` edit in this unit by design. |
| C-006 | Registry row `DF-METADATA-COL-1` appended in-section in `docs/spark-sql-iceberg-parity.md`; example `docs/examples/io/metadata_column.py` (ReparkSession idiom) + `inventory.txt` refresh; EX-0 count in `python/repark-parity/tests` moves by one; 1e6-row parquet `count()` before/after recorded. | Registry diff; example run; EX-0 test green; perf numbers here. | **OPEN** | Step 5. |
| C-007 | No regression: `make verify`, whole facade suite, whole parity suite, `cargo test -p repark-core`, `cargo test -p repark-python`, all green with real exit codes and counts. | §Gates table. | **OPEN** | Step 6. |

## Rulings (binding on this unit)

- **R-1 (card DESIGN).** Per-file `row_index`: one partition per file behind a window
  `row_number() OVER () - 1` inside each per-file branch. DataFusion 54.1 has no parquet
  `row_index` source, so the scan computes it; a global row number is never substituted.
- **R-2 (actor, 2026-09-16).** `metadataColumn` validates eagerly at the call (name shape +
  frame provenance). Every error cell wraps the call inside the use, so eager fits all
  timings; the lazy `repr` cell raises because the call inside it raises. The returned
  Column is a plain stable `col("_metadata")` (repr falls out); join/agg/view/range
  behavior comes from the native provenance walk at use.
- **R-3 (actor, 2026-09-16).** `getCondition()` for native-raised metadata errors works
  through `python/repark/src/repark/errors.py`: the class-level `_native_get_condition`
  (and siblings) become attr-aware, returning `_spark_error_class` /
  `_spark_message_parameters` / `_spark_sql_state` when the instance carries them. Rust
  sets the three attrs (transpose precedent). Zero behavior change for errors without
  the attrs; facade per-instance bindings still shadow.
- **R-4 (actor, 2026-09-16).** `core.py` (exact 4015) and `column.py` (exact 1532) stay
  line-held: one-line `metadataColumn` delegation + one-line `_column_of` funnel, offset
  by precedent-style condensing (df-plan-introspect-1). All other facade work lives in
  the new `dataframe/metadata_column.py` (plus a `MetadataColumn(Column)` subclass for
  the `_metadata.file_name` getField naming the cells pin) and editable files
  (`errors.py`, `_type_table.py`/`types_bases.py` only if the NOT NULL DDL route is taken).
- **R-5 (actor, 2026-09-16).** Nested `nullable:false` flows through the existing
  `logical_schema_fields` → `fromDDL` bridge by rendering `NOT NULL` markers in the
  struct type-key for metadata-marked structs only, parsed by a NOT NULL-tolerant
  `fromDDL` pre-step. No other struct changes shape (markers never occur today — the
  Rust DDL parser rejects them).
- **R-6 (actor, 2026-09-16).** Pre-existing gaps stay out: hive partition columns on the
  base read path (`pp` reads as `[i]` on main) and the `parquet.\`<path>\`` table door
  belong to other units. The metadata path carries partition literals itself so
  `names_parquet_partitioned` answers Spark exactly; the base gap is recorded, not fixed.
- **R-7 (actor, 2026-09-16).** Walk transparency: `Project`/`Filter`/`Sort`/`Limit` pass
  through; `SubqueryAlias` (DF-door `.alias`) passes through; `Join`/`Aggregate` die as
  `MISSING_ATTRIBUTES`; views (`ViewTable`), memtables, non-file scans, unions and
  anything else die as `UNRESOLVED_COLUMN.WITHOUT_SUGGESTION`. Union-of-file-frames is a
  known unpinned residual (Spark keeps metadata; this unit refuses).

## Rust-first roll-call

| Piece | Home | Reason if Python |
|---|---|---|
| File-scan marker provider + provenance walk + per-file UNION augmentation + `__repark_file_metadata[_no_row_index]` struct UDFs + `SparkCondition` errors | `crates/repark-core/src/file_metadata.rs` (new) | — |
| `_metadata` ref detection + rewrite + augmentation hook in `select`/`with_column`/`filter`/`sort`/`aggregate`; `file_metadata_status` for the facade | `crates/repark-python/src/dataframe.rs` (+ new `column/metadata_ref.rs` if it overflows) | — |
| `TableScan` rewrap at read time (parquet/csv/json) + text provider marking | `crates/repark-core/src/session.rs`, `text_scan.rs` | — |
| `NOT_STR` argument check | facade `metadata_column.py` | Pure API plumbing, no value decision (standing-rule ledger line). |
| `metadataColumn` name binding + string-funnel + `MetadataColumn.getField` dot naming | facade `metadata_column.py` | Names, argument shapes, API plumbing only. |
| `getCondition()` attr-aware patch | `errors.py` | Exception-surface plumbing; conditions themselves are raised in Rust. |
| `NOT NULL` struct parsing (if R-5 route) | `_type_table.py`/`types_bases.py` | Parser for a DDL spelling; nullability decided by the Rust UDF. |

## Design (read the scans, 2026-09-16)

Reads flow `DataFrameReader.*` → `crates/repark-python/src/session.rs`
(`read_parquet`/`read_csv`/`read_json`) → `crates/repark-core` `Session::read_parquet`
(`spark_nullable::read_parquet_nullable` over `ctx.read_parquet`), `read_csv`
(`read_options::read_csv_path`), `read_json` (`ctx.read_json`), text (custom provider in
`text_scan.rs`). Every file read ends in a `LogicalPlan::TableScan` whose source is
`DefaultTableSource<ListingTable>` (or the text provider).

1. **Mark.** At read time the `TableScan` is rebuilt with source
   `FileMetadataScan { inner, kind }` (`kind`: parquet/csv/json/text + re-read options).
   Same schema, full delegation — an unreferenced scan pays nothing.
2. **Detect.** Native `select`/`with_column`/`filter`/`sort`/`aggregate` walk exprs for a
   `_metadata` root (`col("_metadata")`, relation-qualified `_metadata.x`, inside
   `get_field` calls). Shadow rule: a scan whose own schema has `_metadata` only honors
   `__metadata` refs (which always miss).
3. **Walk.** From the plan root through transparent nodes to the single marker scan;
   anything else resolves the M-4 error side (R-7). `TableScan` pushdown present →
   dead (never occurs on facade-built plans).
4. **Augment.** File list + `(size, mtime)` come from the physical plan's
   `FileScanConfig` file groups (`input_files` precedent, sync, never executed). Per
   file: single-file re-read through the same core reader, hive partition literals
   (string default), `Repartition(1)`, `row_number() OVER () - 1` (parquet only),
   scalar UDF assembling the non-nullable struct with the
   `__file_source_metadata_col` / `__metadata_col` field metadata. Branches UNION ALL;
   the `TableScan` node is swapped for the augmented subplan, outer exprs unchanged.
5. **Name.** Facade strings alias explicitly (`_metadata` / last segment);
   `F.col("_metadata.x")` keeps the facade alias-free path and the native rewrite emits
   `Alias(get_field(…), x)`; `MetadataColumn.getField` emits projection
   `_metadata.file_name` (cells `metadata_parquet_field`, `metadata_after_filter`).
6. **Perf.** `count()` never references `_metadata`: no augmentation, identical plan.
   1e6-row before/after in step 5.

## §1 Before-table (base `02abfd0e`, probe repark mode, `/tmp/meta-before/`)

`metadata_user_column_shadow` GREEN (user column resolves), `sql_star_hidden` GREEN.
All 32 other run18b cells RED: string refs raise `A column with name … cannot be
resolved` (facade), `F.col` refs raise `Schema error: No field named …` (native, no
condition), every `metadataColumn` use raises `[ATTRIBUTE_NOT_SUPPORTED] Attribute
\`metadataColumn\` is not supported`. SQL door: `sql_path_metadata` →
`table 'datafusion.parquet.…' not found`; `sql_view_metadata` / `sql_local_metadata`
→ `Schema error: No field named …` without condition. Full output kept at
`/tmp/meta-before/repark.json` (outside the repo).

## Cells wanted

None — the 34 run18b cells plus the 14 run-16b `metadata_*` cells cover every contract
sentence. No JVM needed.

## Questions

None in step 1. (Union-of-file-frames and non-string partition inference are recorded
residuals, not questions — R-6/R-7.)

## Gates

| Command | Exit | Counts |
|---|---|---|
| (step 6) | | |

## Coverage attestation

(Step 6, once every clause is PROVEN.)

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED.
