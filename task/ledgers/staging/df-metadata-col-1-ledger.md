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

## §3 Red-first (base `02abfd0e`, `test_df_metadata_col_1.py`, 46 pins)

`40 failed, 5 passed, 1 xfailed`. Green-before: `metadata_user_column_shadow`,
`metadata_select_star_hidden`, `sql_star_hidden`, `sql_view_metadata`,
`sql_local_metadata` (today's shapes, kept as pins). The strict xfail
`test_sql_path_metadata` holds today's `not found` door answer. Representative reds:
`AnalysisException: A column with name '_metadata' cannot be resolved` (facade
strings), `Schema error: No field named _metadata…` (native `F.col`), and
`[ATTRIBUTE_NOT_SUPPORTED] Attribute 'metadataColumn' is not supported` (every
`metadataColumn` use).

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

## Round 2 end-of-turn note (2026-09-16, step budget exhausted, work continues round 3)

Status: 3 reds remain in `test_df_metadata_col_1.py`; all else green per round-2 start
(42 passed / 3 failed / 1 xfailed). The `DuplicateUnqualifiedField i` failure is GONE
(widen/strip-qualifier work holds); remaining reds diagnosed live this round:

1. `test_parquet_values` — nullability index 5 only: `endswith` answers True, oracle
   False. `test_csv_values` — index 3 only: `substring` answers True, oracle False.
   Root cause read from vendored source: DataFusion 54 default
   `return_field_from_args` (`datafusion-expr-54.1.0/src/udf.rs:677-686`) marks every
   scalar-function result nullable; Spark propagates (non-null inputs -> non-null).
   Sanctioned fix path is the `SparkNullability` analyzer
   (`crates/repark-functions/src/spark_nullability.rs`, NULLABILITY-2 precedent):
   wrap `ends_with` / `substring` calls in `spark_nonnull_udf` when DF says nullable
   and no argument field is nullable. `substring` reaches the `SparkSubstring` shim
   via `analyzer.rs:64`; `ends_with` has no shim (DF builtin), so the analyzer wrap
   covers both uniformly. OPEN: whether `spark_nullability.rs` is inside the brief's
   "Rust registry" ownership ban (`registration.rs` surely is; the analyzer-rule file
   arguably is not) — asked as handback Q1.
2. `test_star_plus_fields_names_partitioned` — bare `spark.read.parquet(pp)` reports
   `['i']`; partition column `s` is missing at READ time (before any select), so the
   facade `"*"`-via-`self.columns` expansion cannot include it. M-1 shape tests never
   assert bare columns, only the `_metadata` select schema, so this passed unnoticed.
   OPEN: partition discovery looks like backlog IO-PARQUET-PARTITION-DISCOVERY-1
   scope — asked as handback Q2 (implement here vs strict-xfail).
3. Mechanics note: one `edit_file` append to
   `crates/repark-core/src/file_metadata/tests.rs` (untracked file) verified present
   by `grep -c` then absent two commands later with no intervening writer; no
   concurrent worker on this tree (`ps` shows other lanes on other checkouts). Bash
   heredoc writes persist; round 3 should prefer them and re-verify edits. No
   build was started and no half-fix was committed; tree otherwise as round 2 found it.

## Round 3 notes (2026-09-16, finishing round)

Rulings recorded: R-18b-9 (derived `endswith`/`substring` nullability is
function-nullability work, out of scope as in LOGICAL-WIDTH-1 W-7; compare
`_metadata` field nullability exactly, columns/types/rows for derived, residue
row in ledger plus one registry sentence; no analyzer in this unit), R-18b-10
(base parquet read drops the hive partition column on main; `names_` /
`schema_parquet_partitioned` stay strict-xfail under BACKLOG
IO-PARQUET-PARTITION-DISCOVERY-1; partitioned `_metadata` fields stay green).
A-2: all 18 added `///` lines removed (14 in `dataframe_file_metadata.rs`, 4 in
tracked diffs); two pedantic `# Errors` sites now carry
`#[allow(clippy::missing_errors_doc)]`; pre-existing docs restored verbatim
after a bad bulk strip. A-3: `file_metadata.rs` (1027) split into
`file_metadata/{status,error,udf,augment,ensure}.rs` (root is 28 lines of
`mod`/`pub use`); `dataframe.rs` baseline ratcheted 1016 to 1014 for the
comment-strip shrink. Commit fold: the size hook scans the tree
(`pass_filenames: false`), so no commit could land before the split; the
`refactor(...)` commit folded into the `feat(...)` commit, split verified
instead. Removed three `debug_*` Rust scratch tests plus the `chain` helper:
they build explicitly-aliased stacked projections that trip DataFusion's
`push_down_leaf_projections` (`DuplicateUnqualifiedField i`), a shape the
facade never emits (facade suite 42 green); the real narrowed-reselect /
second-hop test stays. Deviation: `schema_parquet_partitioned` left green, not
strict-xfailed — a strict xfail on a passing test reds the suite; the
partitioned `_metadata` struct answers its own fields per R-18b-10's last line.

## Residue row — derived-expression nullability (R-18b-9, open)

`endswith(...)` / `substring(...)` over non-nullable `_metadata` fields answer
nullable `True`; live PySpark 4.1.2 answers `False` (non-null in, non-null
out). Root cause read from the vendored tree: DataFusion 54 default
`return_field_from_args` (`datafusion-expr-54.1.0/src/udf.rs:677-686`) marks
every scalar-function result nullable. Fixing it is function-nullability work
in run 18a's files, out of this unit like LOGICAL-WIDTH-1 W-7. Disposition:
`test_parquet_values` / `test_csv_values` pin columns, `simpleString`, exact
`_metadata`-field nullability, and all row values; only the derived columns'
nullable flags go uncompared. One sentence mirrors this in registry row
DF-METADATA-COL-1.

## Round 3 fix notes (alias root cause, extractions)

The M-3 row failures were optimizer `push_down_leaf_projections`
`DuplicateUnqualifiedField i`, not nullability: the facade binds every select
string as a quoted same-name alias (`col("\"i\"").alias("i")`), and a redundant
same-name alias on a plain column in the lower of two stacked projections
trips the rule on the hop above (proven by a 4-variant Rust matrix: only
lower-level aliases fail; widen passthrough, casts, UDF shape, and UNION all
exonerated). Fix at the planning boundary (`ensure.rs`): drop a top-level
`Alias(Column)` when the column is unqualified and names match, and emit the
widen passthrough as a bare column. End-to-end: 44 green, 2 xfailed.
Mechanical fallout in the same slices: `sequence_branch` / `empty_branch` /
`collect_hits` leave `augment.rs` (pedantic `too_many_lines`); test-only
re-exports moved out of the module root (non-test builds warn); audit skill
note: divergences re-measured live this round (derived True-vs-False,
partitioned `['i']`-vs-`['i','s']`) and filed as this residue plus BACKLOG
IO-PARQUET-PARTITION-DISCOVERY-1, no new findings.

## COVERAGE_ATTESTATION (2026-09-17, round 3)

- M-1 shape: `schema_parquet/csv/json/text`, `schema_parquet_partitioned`,
  `json_parquet_partitioned` cells green — struct nullable false, every field
  non-nullable. Artifact: `python/repark/tests/test_df_metadata_col_1.py`.
- M-2 names: `names_*` cells green; `names_parquet_partitioned`
  strict-xfailed (BACKLOG IO-PARQUET-PARTITION-DISCOVERY-1).
- M-3 values: `values_parquet/csv` green incl. collected rows
  (columns + `simpleString` + exact metadata-field nullability); derived
  `endswith`/`substring` nullability uncompared (residue row above).
- M-4 `metadataColumn`: `metadata_*` cells green incl. post-select, filter,
  text; plus `docs/examples/dataframe/metadata_column.py` executed rc=0 and
  covered (`DataFrame.metadataColumn`, inventory row, EX-0 enumerator
  1082 → 1083, coverage gate rc=0 with falsification).
- M-5 errors: error-condition cells green in the same file (C-006/C-007).
- Native door: `repark-core` lib `file_metadata` (10) + `plan_introspect`
  (10) tests green. SQL door: path-table door strict-xfailed under
  pre-existing SQL-METADATA-COL-1 (out of scope).
- No `#[expect]`, no EXCEPTIONS row, no comments added; size ceilings held
  (split + 1016 → 1014 ratchet + CAP-1/EX-0 mirrors).

## VERDICT

CONCLUDED with one environmental finding: whole parity suite rc=0
(757 passed); `make verify` rc=0; whole facade suite 9076 passed with 2
failures confined to `test_q14_current_date_bare_and_paren`, a UTC-vs-EDT
midnight-window flake (engine answers UTC 09-17, `date.today()` answers EDT
09-16; both pass under TZ=UTC on this tree; no date code in this diff) —
left red for its owning unit, recorded here and in the hand-back.
`git status --porcelain` must read empty at handoff.
