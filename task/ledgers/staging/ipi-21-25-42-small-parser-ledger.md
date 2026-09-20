# Unit ledger — IPI-21 + IPI-25 + IPI-42 · the small parser shapes

**Date:** 2026-09-20 · **Branch:** `fix/ipi-21-25-42-small-parser` · **Base:** `e4160a58` (`origin/main`)
**Model:** Claude Opus 5 (max) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18 is 1:1 parity with Spark's Iceberg integration, and full
Iceberg parity gates v1.5.0. Three slate rows — IPI-21, IPI-25, IPI-42 — are nine inventory cells
and **one** mechanism: RePark's pre-parse seam refused a keyword on the way to behaviour that
already worked. `CREATE OR REPLACE TABLE [AS SELECT]` is shipped and pinned (registry
`RTAS-OPS-1`); the ref-DDL parser is RePark's own; the fork already owns the purge action
(`DeleteReachableFiles`). Every one of the nine was a refusal, not a missing capability.

**What it is.** Three keyword shapes, each landing on finished behaviour:

- **IPI-42** — `IF NOT EXISTS` parses as an optional **infix** between `BRANCH|TAG` and the ref
  name (Spark's position), `IF EXISTS` likewise after `DROP BRANCH|TAG`. Both guards are
  *conditional*, never blanket no-ops: a missing ref is still created, a present ref is still
  dropped, and an existing ref is **not moved** to a newer snapshot.
- **IPI-25** — `REPLACE TABLE …` is rewritten to `CREATE OR REPLACE TABLE …` as the **first**
  token rewrite of `parse_single_normalized`, so `USING`, `PARTITIONED BY` and the column-type
  rewrites all still run. The one semantic difference between the two spellings — `REPLACE TABLE`
  requires the table to exist — is enforced by an existence pre-check in the router.
- **IPI-21** — a `PURGE` token survives the Python `DROP` expander and is threaded from
  `Statement::Drop.purge` into `execute_drop_table`, which sweeps the reachable files with the
  fork's `DeleteReachableFiles` **before** the catalog drop, gated on `gc.enabled`.

**Not in this unit:** `STATUS.md`; `Cargo.toml` / `Cargo.lock` / the fork pin; any size-ceiling
change; the error-CLASS gaps listed under Residues (IPI-51 owns them); plain `DROP TABLE` on a
missing table (its class gap is IPI-51's, and this unit does not change that path).

**Writable paths:** `crates/repark-spark/src/` (the parser, the router seam, the purge module and
their tests), `python/repark/src/repark/spark/session/` (the `DROP` expander and its regexes),
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`, this ledger, touched `map.md` files.

## Measured

Oracle: the run-25/26 inventory harness against live PySpark 4.1.2 +
`iceberg-spark-runtime-4.1_2.13:1.11.0` (`/tmp/oc-worker/nc-inventory/matrix.json`, cells below),
plus the run-25e probe `/tmp/oc-worker/qe/probe/p3.json` (keys `E.*`) for the shapes no cell
covers. Nothing here is inferred from Java source; every row is a recorded answer.

| Cell / probe key | Spark's recorded answer |
|---|---|
| `D-REF-CREATE-BRANCH-IF-NOT-EXISTS` | no error, no change — `md.refs` `[["b1","branch","S1"],["main","branch","S1"]]` |
| `D-REF-TAG-IF-NOT-EXISTS` | the `AS OF VERSION S0` is **ignored**; `t1` stays at `S1` |
| `D-REF-DROP-BRANCH-IF-EXISTS` | no error; `md.refs` `[["main","branch","S1"]]` |
| `D-REF-DROP-TAG-IF-EXISTS` | no error; `md.refs` unchanged |
| `E.create_branch_if_not_exists_missing` / `E.refs_after` | the guard still **creates** `bnew` |
| `E.drop_branch_if_exists_present` | the guard still **drops** a present ref |
| `E.create_branch_existing_no_if` | `IllegalArgumentException: Ref b1 already exists` |
| `E.drop_branch_missing_no_if` | `IllegalArgumentException: Branch does not exist: nope` |
| `E.create_or_replace_branch_if_not_exists` / `E.replace_branch_if_not_exists` | **parse error**: `mismatched input 'NOT' expecting {<EOF>, 'AS', 'RETAIN', 'WITH'}` — `IF NOT EXISTS` is grammatical only after a **plain** `CREATE BRANCH|TAG` |
| `D-REPLACE` | schema `[["k","int"],["v","string"]]`; `md.refs` `[]`; one snapshot (`append`); `data` `[]` |
| `D-RTAS` | schema `[["id","long"]]`; `[append, overwrite]`; `md.refs` `[["main","branch","S1"]]`; rows `[[0],[10]]` |
| `D-RTAS-TIME-TRAVEL` | the pre-replace snapshot still reads `[[0,"d0","a"],[1,"d1","b"],[2,"d2","a"]]` |
| `E.replace_missing` / `E.replace_missing_rtas` | `AnalysisException [TABLE_OR_VIEW_NOT_FOUND] … SQLSTATE: 42P01` |
| `D-DROP-TABLE-PURGE` | `data_files_exist_after: [false]` — the data files are gone |
| `D-DROP-TABLE-NO-PURGE` | `data_files_exist_after: [true]` — plain `DROP TABLE` **keeps** them |
| `TP-GC-DISABLED-PURGE` | `org.apache.iceberg.exceptions.ValidationException: Cannot purge table: GC is disabled (deleting files may corrupt other tables)` |
| `E.drop_table_purge_missing` | `[TABLE_OR_VIEW_NOT_FOUND]` |
| `E.drop_table_if_exists_purge` | ok — `PURGE` composes with `IF EXISTS` |

**Reversed before a line was written.** The packet's D-3 and `INDEX.md` decision 28 ruled *no
`gc.enabled` gate* on the strength of "no cell measures it" and the fork module doc's note that
Java does not gate `DeleteReachableFiles`. Both are wrong for the SQL path: `units.py` assigns
IPI-21 **two** cells, and the second (`TP-GC-DISABLED-PURGE`) records Spark **refusing**. Java's
`SparkCatalog.purgeTable` gates before it ever reaches the action. Shipping without the guard
would have deleted the user's data files where Spark keeps them — a DIFFERENT verdict and real
data loss. The guard is implemented and pinned; the mutant that omits it goes red.

## Clauses

| Id | Clause | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `CREATE BRANCH\|TAG IF NOT EXISTS` on an **existing** ref is a no-op: no error, no metadata change, and the ref is **not** moved to a newer snapshot — an `AS OF VERSION` on the guarded form is ignored. | Cell pins that pin the ref at an OLDER snapshot first, so a replace-if-different implementation moves it and reds. | PROVEN | `crates/repark-spark/src/tests/ref_ddl.rs::ref_guards_are_conditional_and_never_move_an_existing_ref` (b1 pinned at the OLDER snapshot first); `python/repark/tests/test_ice_small_parser_1.py::{test_create_branch_if_not_exists_is_noop,test_create_tag_if_not_exists_with_version_is_noop}` |
| C-002 | The same guard on a **missing** ref still creates it, for `BRANCH` and for `TAG`, at the current snapshot or at an explicit `AS OF VERSION`. | Conditional pins on both kinds; a blanket no-op reds. | PROVEN | `crates/repark-spark/src/tests/ref_ddl.rs::guarded_create_and_drop_apply_to_missing_refs`; `python/repark/tests/test_ice_small_parser_1.py::{test_create_branch_if_not_exists_missing_creates_it,test_create_tag_if_not_exists_missing_creates_it,test_create_tag_if_not_exists_missing_honours_as_of_version}` |
| C-003 | `DROP BRANCH\|TAG IF EXISTS` on a **missing** ref is a no-op and on a **present** ref still drops it, for both kinds. | Conditional pins on both kinds and both branches. | PROVEN | `crates/repark-spark/src/tests/ref_ddl.rs::guarded_create_and_drop_apply_to_missing_refs` (the guarded-drop arms); `python/repark/tests/test_ice_small_parser_1.py::{test_drop_branch_if_exists_missing_is_noop,test_drop_tag_if_exists_missing_is_noop,test_drop_branch_if_exists_present_drops_it,test_drop_tag_if_exists_present_drops_it}` |
| C-004 | The guardless forms still refuse (`already exists` / `does not exist`); an unknown trailing clause still refuses naming the leftover token; `CREATE OR REPLACE … IF NOT EXISTS` and `REPLACE … IF NOT EXISTS` refuse parse-class; the top-level `… IN cat.ns.t` spellings take the same infix. | The narrowed refusal keeps its guard; Spark's own parse refusal is matched in class. | PROVEN | `crates/repark-spark/src/tests/ref_ddl.rs::ref_ddl_if_exists_spellings_run_and_unknown_trailing_clauses_still_refuse`; `crates/repark-spark/src/ref_ddl/tests.rs::{parses_if_not_exists_infix_on_create,parses_if_exists_infix_on_drop,postfix_if_not_exists_still_refuses,or_replace_with_if_not_exists_refuses_parse_class}`; `python/repark/tests/test_ice_small_parser_1.py::{test_create_branch_existing_without_guard_raises,test_drop_branch_missing_without_guard_raises,test_or_replace_with_if_not_exists_refuses}` |
| C-005 | `REPLACE TABLE t (cols) USING iceberg` is the column-def `CREATE OR REPLACE` path: schema replaced, `md.refs` `[]`, **no new snapshot**, zero rows. | The `D-REPLACE` observables asserted key by key. | PROVEN | `crates/repark-spark/src/tests/replace_table.rs::replace_table_column_list_takes_the_column_def_replace_path`; `python/repark/tests/test_ice_small_parser_1.py::test_replace_table_column_list` |
| C-006 | `REPLACE TABLE … AS SELECT` is the RTAS path: `[append, overwrite]`, `main` at the new snapshot, the projected rows — and the pre-replace snapshot is still readable by `VERSION AS OF`. | `D-RTAS` and `D-RTAS-TIME-TRAVEL`; the `overwrite` stamp is what a plain-`CREATE TABLE` rewrite would lose. | PROVEN | `crates/repark-spark/src/tests/replace_table.rs::replace_table_as_select_records_an_overwrite`; `python/repark/tests/test_ice_small_parser_1.py::{test_replace_table_as_select,test_replace_table_preserves_time_travel}` |
| C-007 | `REPLACE TABLE` on a **missing** table refuses with Spark's `[TABLE_OR_VIEW_NOT_FOUND]` and `SQLSTATE: 42P01` on both spellings and creates nothing; `CREATE OR REPLACE TABLE` is unchanged on all three of its arms. | The one behavioural difference between the two spellings, pinned; plus the regression pin. | PROVEN | `crates/repark-spark/src/tests/replace_table.rs::{replace_table_on_a_missing_table_refuses_and_creates_nothing,create_or_replace_table_still_creates_a_missing_table}`; `python/repark/tests/test_ice_small_parser_1.py::{test_replace_table_missing_raises_table_not_found,test_create_or_replace_table_unchanged}` |
| C-008 | `DROP TABLE … PURGE` parses through the Python expander and the Rust router and deletes every reachable data file before the catalog drop; `IF EXISTS` composes both ways; a missing target refuses `[TABLE_OR_VIEW_NOT_FOUND]` / `42P01`. | The expander rewrite pinned byte-exact; the file-existence pins; the missing-table pins. | PROVEN | `crates/repark-spark/src/tests/purge.rs::{drop_table_purge_deletes_reachable_files_and_plain_drop_keeps_them,purge_composes_with_if_exists_and_names_a_missing_table_the_spark_way}`; `python/repark/tests/test_ice_small_parser_1.py::{test_drop_expander_emits_purge,test_drop_table_purge_deletes_data_files,test_drop_table_if_exists_purge_present_deletes_data_files,test_drop_table_if_exists_purge_missing_is_ok,test_drop_table_purge_missing_raises_table_not_found}` |
| C-009 | Plain `DROP TABLE` — with or without `IF EXISTS` — **never** purges: every recorded data file still exists afterwards. | The regression pin that a purge-by-default bug reds, and nothing else would. | PROVEN | `crates/repark-spark/src/tests/purge.rs::drop_table_purge_deletes_reachable_files_and_plain_drop_keeps_them` (the plain-drop arm); `python/repark/tests/test_ice_small_parser_1.py::{test_drop_table_without_purge_keeps_data_files,test_drop_table_if_exists_without_purge_keeps_data_files}` |
| C-010 | `gc.enabled=false` refuses the purge with Spark's text, sweeps nothing and drops nothing; per-file delete failures are collected by the sweep and logged at the door — one `tracing::warn` naming the table and the count — never surfaced in the SQL or DataFrame result (Java's log-only suppression). | The property pin on both values, and a Rust pin that fails one path's delete and still sees the table gone. | PROVEN | `crates/repark-spark/src/tests/purge.rs::{purge_refuses_when_gc_is_disabled_and_sweeps_nothing,purge_collects_delete_failures_and_the_drop_still_runs}`; `python/repark/tests/test_ice_small_parser_1.py::{test_drop_table_purge_gc_disabled_refuses,test_drop_table_purge_gc_enabled_true_still_purges}` |

## Residues

Numbered, with their cell names. None is a footnote; each is a gate item owned by another row.

1. **`TP-GC-DISABLED-PURGE` error class.** Spark raises `Py4JJavaError` wrapping
   `org.apache.iceberg.exceptions.ValidationException`; RePark raises `AnalysisException`. The
   **text** matches. The class gap is IPI-51 (error conditions), not this unit.
2. **`D-REF-DROP-BRANCH-IF-EXISTS` sibling text.** The *guardless* `DROP BRANCH nope` refusal
   reads `Ref nope does not exist` (the fork's `ManageSnapshots` text) where Spark reads
   `Branch does not exist: nope`, and its class is not `IllegalArgumentException`. No cell
   measures the guardless form; IPI-51 owns the text and class.
3. **`D-DROP-TABLE-MISSING-ERR`.** Plain `DROP TABLE <missing>` (no `PURGE`) still answers
   RePark's `TableNotFound` where Spark answers `[TABLE_OR_VIEW_NOT_FOUND]`. This unit changes
   only the `PURGE` path, which now matches; the non-`PURGE` gap is IPI-51's.
4. **`E.create_or_replace_branch_if_not_exists` caret block.** Spark's parse refusal carries an
   `== SQL ==` caret block; RePark's parse-class refusal does not. The formatter is IPI-51's
   D-2.3; the class and the refusal itself are matched here.
5. **Door-level delete-failure mutant.** A deleter that `Err`s on a non-empty
   `delete_failures` at the SQL door is not killed: the only pin driving a failing
   deleter sits at the `DeleteReachableFiles` level
   (`tests/purge.rs::purge_collects_delete_failures_and_the_drop_still_runs`) and there
   is no injection channel through the session. Accepted as residue per owner ruling
   2026-09-20 10:11 (no production-surface change in this unit) — the door logs the
   count once and the `DROP` still succeeds, Java's posture exactly.

## Pointers

- Slate row: [../../roadmap/mid-term/ice-parity-inventory-2026-09-19.md](../../roadmap/mid-term/ice-parity-inventory-2026-09-19.md) rows 53, IPI-21, IPI-25.
- Registry: [../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md) — `REF-2` retired, `RTAS-OPS-1` widened, `ICE-DROP-PURGE-1` filed.
- Ledger index: [map.md](map.md).
