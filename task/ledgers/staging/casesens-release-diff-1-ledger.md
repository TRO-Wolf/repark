# Unit ledger — WO CASESENS-RELEASE-DIFF-1 · two v1.5.1 release-differential regressions

**Date:** 2026-09-28 · **Branch:** `fix/casesens-release-diff-1` · **Base:** `746c0fd3`
(#881 CASESENS-2 head) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** GUIDED. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** The v1.5.1 release differential (5331 statements, Spark 4.1.2 +
Iceberg 1.11.0) found two regressions from CASESENS-1: RD-1, a case-twin frame
writes through the DataFrameWriter path door under `caseSensitive=false`
(local and s3a) where Spark refuses `COLUMN_ALREADY_EXISTS`; RD-2, an
exact-mode qualified DataFrame miss raises a classless Schema error where
Spark (and v1.5.0) raise `UNRESOLVED_COLUMN.WITH_SUGGESTION`.

**Step 0.** The first round halted well: live Spark overturned two brief
premises — csv twin writes succeed on Spark in every mode and header setting,
and Spark's twin notion is Unicode-aware (é/É twins, ß/SS distinct) on the
path, CTAS and view doors. Q1 ruled per-format scope (parquet/json refuse,
csv writes, orc keeps its declared refusal, nothing refuses under true); Q2
ruled no matcher change (`folded_duplicate` as on this branch, Java
equalsIgnoreCase). The lane then moved to #881's head, which also fixed PE-5
and the string-select miss on the base tree.

**What Spark does (measured 2026-09-28; Spark 4.1.2 + Iceberg 1.11.0 hadoop
catalog, UTC banner; local probes plus moto s3a).** Under false, twin
parquet/json writes refuse `COLUMN_ALREADY_EXISTS` (42711) naming the folded
`` `a` `` with nothing written, in every save mode including ignore-on-existing
and append-on-existing, and before any mode handling; twin csv writes in every
mode and header setting (file bytes `1,2` unset/false, `a,A`/`1,2` with header);
twin orc refuses the same class; é/É twins refuse naming `` `é` `` on the
path, CTAS and view doors while ß/SS writes; `partitionBy` naming a twin
refuses `AMBIGUOUS_REFERENCE` (42704); a respelled partition column writes a
`ID=1` dir under false and refuses legacy `_LEGACY_ERROR_TEMP_1155` under
true. Under true twin writes land and read back on every format including
s3a. The RD-2 miss raises `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) naming
`` `t`.`ID` `` with suggestions `` [`ID`, `data`] `` under both settings, as
does the bare miss with `` [`idx`, `data`] ``; the aliased-table binds-case
answers under false. Oracle: `casesens_release_diff_1_spark.json`, 24 cells.

**Fix.** RD-1: one shared call site at the top of
`DataFrameWriter._apply_path_write` (before mode handling, covering local and
S3) runs the existing `refuse_folded_duplicate_keys` native entry — the same
`folded_duplicate` CTAS and views use — gated to parquet/json in the new
`writer_layout.refuse_path_write_twins` helper. RD-2: `bind_names` under
`Exact` routes every total miss through the existing `unresolved_column`
translation instead of passing it to DataFusion's raw error; safe for
subquery-inner columns because `Expr::transform` treats subqueries as leaves
(DataFusion 54 `tree_node.rs` verified). The `false` path is untouched.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Under false, twin parquet/json path writes refuse `COLUMN_ALREADY_EXISTS` (42711) naming `` `a` `` with nothing written: direct, `save(format)`, ignore-on-existing (seed unchanged), partitioned-by-clean-column, and one moto s3a cell with zero objects stored. | Facade replay asserts type, condition, SQLSTATE and head per key. | PROVEN | `test_casesens_release_diff_1.py::test_twin_parquet_and_json_refuse_and_write_nothing`, `::test_twin_save_with_format_refuses`, `::test_twin_parquet_refuses_before_save_modes`, `::test_twin_frame_with_clean_partition_column_refuses`, `::test_twin_parquet_refuses_on_s3a` (18/18 file green); M1 reds all five. |
| C-002 | Under false, twin csv writes keep writing with pinned file bytes and read-back per header setting (unset/header/nohdr); twin orc keeps its declared `NOT_IMPLEMENTED` refusal; under true twin writes proceed and read back by name on parquet/json/csv; twin-free frames write every format. | Facade replay asserts bytes, rows and the orc refusal. | PROVEN | `::test_twin_csv_writes_with_header_settings`, `::test_twin_orc_keeps_its_declared_refusal`, `::test_twin_writes_proceed_under_true`, `::test_plain_frame_writes_every_format` green; M1 keeps all four green. |
| C-003 | Non-ASCII twins follow Spark's shape (é/É refuses 42711, ß/SS writes); twin saveAsTable and CTAS keep refusing; `partitionBy` naming a twin refuses with nothing written; a respelled partition column writes under false. | Facade replay asserts the refusals, rows and dirs. | PROVEN | `::test_non_ascii_twins_follow_spark`, `::test_save_as_table_and_ctas_twins_still_refuse`, `::test_partition_by_naming_a_twin_refuses`, `::test_partition_by_respelled_column_writes` green; M1 reds the first and third. Residues R-3 (name), R-1 (class), R-4 (dir). |
| C-004 | Under true, the exact `F.col` qualified and bare misses raise `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) with Spark's head and candidate set, and exact hits still bind; the aliased-table binds-case answers under false; false-path misses stay byte-identical; the string-select miss keeps refusing 42703. | Facade replay asserts class, head and candidates, plus exact raw text. | PROVEN | `::test_exact_qualified_miss_keeps_unresolved_column`, `::test_exact_bare_miss_keeps_unresolved_column`, `::test_exact_string_miss_keeps_unresolved_column`, `::test_qualified_name_binds_where_it_matches`, `::test_false_path_misses_stay_byte_identical` green. |
| C-005 | Nothing regresses: the 25-statement neighbour probe (writes plus qualified names, `cs_probe4/6/9/10/11` shapes) is 25/25 unchanged base-to-head; zero existing parity pins change; the touched-crate lib suites and the adjacent facade suites stay green. | Neighbour rerun plus the scoped sweeps. | PROVEN | Neighbour base/head diff 0/25; `cargo test -p repark-core --lib` 887 green, `-p repark-python --lib` 85 green, `-p repark-spark --lib` green; `test_casesens_1/2` 94, readwriter+s3+surfaces+filter 216, true-mode suites 313 green. |

## Mutation record (2026-09-28)

| # | Mutation | Red |
|---|---|---|
| M1 | The `_apply_path_write` seam call replaced with `pass` (Python only, no rebuild). | The 7 RD-1 refuse-pins red (parquet/json, save-format, before-modes, clean-partition, non-ascii, partition-twin, s3a); the 11 others stay green including under-true; restored, `git status` clean of the mutation. |

## Tests rewritten

One unit-test leg: `case_bind.rs::exact_rule_refuses_a_case_only_match` pinned
`bind_names(col("nope"), Exact)` passing through to DataFusion's raw error;
the brief's RD-2 fix requires the indistinguishable `t.ID` total miss to raise
`UNRESOLVED_COLUMN`, so the leg now asserts the `UNRESOLVED_COLUMN` text with
suggestions `` [`id`, `Data`, `s`] `` — toward Spark's measured bare-miss
answer. No parity pin changed.

## Residues

| # | Residue |
|---|---|
| R-1 | **OPEN 2026-09-28:** `partitionBy` naming a twin refuses `COLUMN_ALREADY_EXISTS` here, `AMBIGUOUS_REFERENCE` on Spark. Partition-column resolution runs after the twin check and detects no twin ambiguity on either door; the ruling accepts pin-as-refusing. Home: path-write partition resolution. |
| R-2 | **OPEN 2026-09-28:** false-path `F.col` misses keep the classless DataFusion text (`Schema error: No field named …`) while Spark raises `UNRESOLVED_COLUMN` under false too. Left byte-identical by ruling; pinned exact. Home: false-path miss translation. |
| R-3 | **OPEN 2026-09-28:** the é/É twin refusal names `` `É` `` (later spelling, ASCII-lowered) here, `` `é` `` (folded) on Spark, on the path, CTAS and view doors alike — the shared `folded_duplicate` report, not widened by ruling. Home: the shared matcher report. |
| R-4 | **OPEN 2026-09-28:** a respelled partition column writes an `id=1` dir here, `ID=1` on Spark. Pre-existing; the write outcome and rows pin. Home: path-write partition naming. |
| R-5 | **OPEN 2026-09-28:** csv twin write without a header option emits an `a,A` header here, none on Spark (carded as CSV-HEADER-DEFAULT-1); read-back pins the actual rows. Home: that card. |
| R-6 | **OPEN 2026-09-28:** s3a twin parquet under true fails with an internal temp-table error (`_repark_s3_write_*` not found) here, writes on Spark. Verified identical at base via stash-rebuild; pre-existing. Home: S3 path-write under true. |
| R-7 | **OPEN 2026-09-28:** `partitionBy` a respelled column under true writes here, Spark refuses legacy `_LEGACY_ERROR_TEMP_1155`. Recorded only, no pin. Home: path-write partition resolution under true. |
| R-8 | **CLOSED 2026-09-28:** CASESENS-2 R-CS2-3 (`F.col("nope")` under true keeps the raw miss text) is closed by this unit's RD-2 fix — exact total misses now raise `UNRESOLVED_COLUMN`. Noted here; that ledger stays its owner's to amend. |

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: casesens-release-diff-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All 5 clauses walked against behavior — the 24-cell Spark 4.1.2 oracle (local plus moto s3a, banner quoted) plus the RePark base column, facade replay per clause, mutation M1 red-then-green, neighbour base/head diff.
      artifacts: [task/ledgers/staging/casesens-release-diff-1-ledger.md, python/repark/tests/test_casesens_release_diff_1.py, python/repark/tests/casesens_release_diff_1_spark.json]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — parquet/json/csv/orc, save-modes error/ignore/append/overwrite, fresh/existing paths, save-versus-direct doors, local versus s3a, twin/non-twin/partitioned frames, é/ß pairs, qualified/bare/string misses under both settings.
      artifacts: [python/repark/tests/test_casesens_release_diff_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every changed refusal raises Spark's measured text (byte-exact, or head plus candidate set, or type plus condition plus state); refusals precede any file creation and leave seeded destinations byte-identical.
      artifacts: [python/repark/tests/test_casesens_release_diff_1.py]
    - id: AT-4
      status: N/A
      justification: No shared or global state touched — the rule reads from the frame's own task_ctx per call, each pin runs in its own session and warehouse, the moto cell owns its server and bucket.
    - id: AT-5
      status: ATTACKED
      evidence: S3 touches run against moto only with testing credentials and null config files; the moto server starts per-module on a free port and stops at teardown; no secret appears in any output.
      artifacts: [python/repark/tests/test_casesens_release_diff_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per leg (collect plus columns/dtypes); divergences recorded as dated residues R-1…R-7 with homes, never absorbed; R-8 closes a prior residue with its home named.
      artifacts: [python/repark/tests/test_casesens_release_diff_1.py, task/ledgers/staging/casesens-release-diff-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Planning-only changes — the twin check runs once per path write, the binder arm replaces a DataFusion error with a cheaper one; file-size ceilings hold with no exception (writer_readwriter.py 997/1000, writer_layout.py grows inside the default).
      artifacts: [scripts/check_lib_py.py, scripts/check_rust_file_size.py]
    - id: AT-8
      status: ATTACKED
      evidence: No new matcher and no new native entry — the seam reuses the existing refuse_folded_duplicate_keys binding; the crate DAG is unchanged; map.md lockstep in every touched directory.
      artifacts: [python/repark/src/repark/spark/dataframe/writer_layout.py, python/repark/src/repark/spark/dataframe/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every changed refusal text is pinned on the door that raises it (byte-exact, head plus candidate set, or exact raw text); the engine planning prefix is stripped by the shared _plain helper and the remainder asserted, not stripped silently.
      artifacts: [python/repark/tests/test_casesens_release_diff_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — the base tree answers every must-change cell wrong (recorded Step 0 base column) and M1 turns the 7 refuse-pins red while the under-true pin stays green; no dead branch ships (each new arm has a named pin).
      artifacts: [task/ledgers/staging/casesens-release-diff-1-ledger.md, python/repark/tests/test_casesens_release_diff_1.py]
```
