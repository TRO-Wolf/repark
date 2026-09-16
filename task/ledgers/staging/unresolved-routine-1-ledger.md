# Charter ledger — UNRESOLVED-ROUTINE-1 · every unknown function refuses with Spark's `UNRESOLVED_ROUTINE` on both doors

**Date:** 2026-09-16 · **Branch:** `feat/unresolved-routine-1` · **Base:** `origin/main`
`33c87cbf` · **Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Q-17c-3 (owner, 2026-09-16): one fix corrects every missing name at
once, ahead of adding more names. The SQL door refuses every unknown routine as
DataFusion's `Invalid function 'x'.`; only `F.call_function` answers Spark's
`UNRESOLVED_ROUTINE` today, and it does so in Python. The blanket path is one
contract, not one row per name.

**Not in this unit:** `functions*.py`, `dataframe/**`, `column.py`, `catalog.py`,
`types.py` (runs 18a/18b); `spark_literals.rs`, `spark_typed.rs`, `type_table.rs`
(run 18c SQL-LITERAL-TYPING-1 lane); any new kernel; LATERAL VIEW parsing
(SPARK-SQL-GRAMMAR-1 C-009, not on main).

## Rulings recorded at open

- Q-17c-3 (owner, 2026-09-16): a 1.5 card, ahead of adding more names — one fix
  corrects every missing name at once.
- Q-17a-2 (owner, 2026-09-16): a decision that raises, casts, coerces or branches
  on a value is a kernel too — if the SQL door cannot reach it, it is in the
  wrong place; Python holds names, argument shapes and API plumbing only. This
  unit's mapping therefore lives in `crates/repark-spark` (reachable from the SQL
  door, the `F.expr` door, and the string-`filter` door through the native
  boundary), not in Python.
- Q-15c-6 (owner): narrow per-function fix, no registry-wide wrapper that
  re-binds every UDF's return field. The mapping matches only the unknown-routine
  error texts and passes everything else through byte-identical.
- R-18c-1 (this lane, 2026-09-16): engine `AnalysisException.getCondition()` stays
  `None` by pinned contract (`test_e1_errorclass.py` pins `None` for constructed
  natives and for engine analysis errors); the class and SQLSTATE travel in the
  message. Making the accessor parse the message is a cross-cutting contract
  change for the error-surface owner — recorded as P2 hand-off H-002, not done
  here. Pins assert the full message text.
- R-18c-2 (this lane, 2026-09-16): `selectExpr` wraps the fragment as
  `SELECT <fragment> FROM <view>` before the native boundary, so the Rust mapping
  sees position 7 where Spark records position 0 relative to the fragment. The
  class is pinned from Rust; the fragment-relative rewrite is a P2 hand-off to
  run 18b (owns `dataframe/core.py`), H-001. Not guessed from Rust: matching on
  the scratch view name would couple the engine to facade internals.
- R-18c-3 (this lane, 2026-09-16): the `system.builtin` qualifier rule is the one
  measured shape (`UR-SQL-05`); wider namespace generalization is unmeasured and
  stays a blanket `UNRESOLVED_ROUTINE` outside that exact qualifier.
- Skill `audit-repark-parity` was not re-run: no JVM may start on this lane, so
  the live oracle is not re-measured. The recorded fixtures named in the brief
  are the spec; every RePark-side value below was observed live on the release
  native built from this branch's base.

## PROPOSITION LEDGER — UNRESOLVED-ROUTINE-1 — 2026-09-16

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The SQL door refuses every unknown scalar/aggregate/window routine as `AnalysisException` carrying `[UNRESOLVED_ROUTINE]`, the name rendered per door rules, the search path, `SQLSTATE: 42883`. Cells UR-SQL-00…04, 06…13, 15. | `test_unresolved_routine_1.py` full-message pins per cell. | OPEN | RED §1. Seam: sql-text-aware mapping of DataFusion's `Invalid function` at the native `sql` boundary. Name case and position recover from the SQL text because DataFusion folds to lowercase. |
| C-002 | The query context `; line L pos P` answers per cell. | Same pins assert the position suffix. | OPEN | Reachable: sqlparser is not needed — the routine-name span is found by a case-insensitive call-site search in the original SQL text. `F.expr`/`filter` search the fragment (pos 0); `selectExpr` needs the facade (R-18c-2, H-001). |
| C-003 | The Python door: `F.expr`, `selectExpr`, string `filter` answer the same class; `F.call_function` stays green. Cells UR-PY-00…03. | Same pin file, py-door section. | OPEN | RED §1 (`F.call_function` already green). `F.expr` maps in `PyColumn::sql`, `filter` in `filter_sql`, both Rust. `functions_byname.py` can delegate to the Rust path — hand-off H-003 to run 18a, not an edit. |
| C-004 | The three out-of-shape classes: TVF implemented at the same seam; LATERAL VIEW and `system.builtin` recorded. | TVF pin UR-SQL-17; BACKLOG rows for the rest as decided. | OPEN | RED §1. TVF is the same seam (`table function 'x' not found` at `session.sql`). `system.builtin` (UR-SQL-05) is the same seam too and is implemented, not backlogged — one measured shape. LATERAL VIEW parses in another unit — BACKLOG only. |
| C-005 | Blanket, not per name: a Rust unit test maps an arbitrary unknown name; one pin iterates ≥20 Spark-unknown names. | In-module Rust test + parametrized facade pin. | OPEN | No name list anywhere in the implementation; the 20-name pin guards the blanket. |
| C-006 | Every existing `Invalid function` assertion for a Spark-unknown name now asserts the Spark class; names Spark has are listed. | Retired pins + the two ledger lists. | OPEN | Files: `test_fnp11b_typeof.py`, `test_fnp7_try_inversions.py`, `test_functions_gt2.py`, `test_filter_predicate_rewrite.py` (docstring only, no assertion), `test_w0_window_bench_smoke.py`, `test_fnp_gen_1.py` (negative assertion, untouched), `test_column_parity_1.py`, Rust `keyword_lower.rs` / `bare_nullary.rs`. |
| C-007 | Registry: BL-19 → FIXED with pins; residue rows dated. | `docs/spark-sql-iceberg-parity.md` rows. | OPEN | BL-19-POS-SELX (selectExpr fragment position, hand-off H-001) plus LATERAL VIEW BACKLOG row if not already rowed. |

## 1. Red-first record (base `33c87cbf`, release native from base, 2026-09-16)

Probe: the brief's `msg.py` over `b2-oracle.json` with the clone `.venv`
(`UR` prefix). Result: every in-scope cell `NE` except `UR-PY-00` (`EQ`).

| Cell | Oracle (Spark 4.1.2) | RePark on base | State |
|---|---|---|---|
| UR-SQL-00 `SELECT nosuchfn(1)` | `[UNRESOLVED_ROUTINE] … \`nosuchfn\` … SQLSTATE: 42883; line 1 pos 7` | `AnalysisException: Error during planning: Invalid function 'nosuchfn'.` | RED (C-001, C-002) |
| UR-SQL-01 ``SELECT `nosuchfn`(1)`` | same, pos 7 | same `Invalid function` | RED |
| UR-SQL-02 `SELECT NoSuchFn(1)` | name keeps case `` `NoSuchFn` ``, pos 7 | `Invalid function 'nosuchfn'.` (folded, case lost) | RED (case recovery needed) |
| UR-SQL-03 `SELECT nosuchfn()` | same, pos 7 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-04 `SELECT spark_catalog.default.nosuchfn(1)` | `` `spark_catalog`.`default`.`nosuchfn` ``, pos 7 | `Invalid function 'spark_catalog.default.nosuchfn'.` | RED |
| UR-SQL-05 `SELECT system.builtin.nosuchfn(1)` | `[REQUIRES_SINGLE_PART_NAMESPACE] … got \`system\`.\`builtin\`. SQLSTATE: 42K05` (no position) | `Invalid function 'system.builtin.nosuchfn'.` | RED (C-004) |
| UR-SQL-06 `SELECT abs(nosuchfn(1))` | pos 11 | `Invalid function 'nosuchfn'.` | RED (nested position) |
| UR-SQL-07 `SELECT 1 WHERE nosuchfn(1) = 1` | pos 15 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-08 `… GROUP BY id` | pos 7 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-09 `… OVER ()` | pos 7 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-10 `SELECT fortnight(1)` | `` `fortnight` `` pos 7 | `Invalid function 'fortnight'.` | RED |
| UR-SQL-11 `SELECT   nosuchfn(1)` | pos 9 | `Invalid function 'nosuchfn'.` | RED (whitespace) |
| UR-SQL-12 `SELECT 1,\n nosuchfn(2)` | line 2 pos 1 | `Invalid function 'nosuchfn'.` | RED (multi-line) |
| UR-SQL-13 `SELECT nosuch.fn(1)` | `` `nosuch`.`fn` `` pos 7 | `Invalid function 'nosuch.fn'.` | RED |
| UR-SQL-14 `SELECT typeof(1)` | ok | ok | CONTROL (Spark has the name; must keep resolving) |
| UR-SQL-15 `SELECT count(nosuchfn(1))` | pos 13 | `Invalid function 'nosuchfn'.` | RED |
| UR-SQL-16 LATERAL VIEW generator | `[ROUTINE_NOT_FOUND] … SQLSTATE: 42883; line 1 pos 23` | `UnsupportedOperationException: … LATERAL VIEWS` | RED → BACKLOG (grammar unit owns the parse) |
| UR-SQL-17 `SELECT * FROM nosuchtvf(1)` | `[UNRESOLVABLE_TABLE_VALUED_FUNCTION] … SQLSTATE: 42883; line 1 pos 14` | `AnalysisException: Error during planning: table function 'nosuchtvf' not found` | RED (C-004, same seam) |
| UR-PY-00 `call_function` | no position | EQ | GREEN, keep |
| UR-PY-01 `F.expr('nosuchfn(1)')` | pos 0 | `Invalid function 'nosuchfn'.` | RED (C-003) |
| UR-PY-02 `selectExpr('nosuchfn(id)')` | pos 0 | `Invalid function 'nosuchfn'.` | RED class (C-003); position → H-001 |
| UR-PY-03 `filter('nosuchfn(id) = 1')` | pos 0 | `Invalid function 'nosuchfn'.` | RED (C-003) |

## 1b. Red-run evidence (step 3, base native)

`.venv/bin/python -m pytest python/repark/tests/test_unresolved_routine_1.py -q -p no:cacheprovider`
→ `41 failed, 4 passed`. The 4 passes are the already-green `UR-PY-00`
(`call_function`), the LATERAL VIEW backlog disclosure, the `typeof` control,
and the fixture-presence check. Sample failure (`UR-SQL-00`):

`assert "Error during planning: Invalid function 'nosuchfn'.\nDid you mean 'cosh'?" == '[UNRESOLVED_ROUTINE] …'`

Finding F-001: DataFusion appends a `\nDid you mean 'x'?` suggestion whose
target varies per name (`cosh`, `count`, `try_validate_utf8` in the probe), so
the name parser reads up to the closing quote and ignores the tail. The blanket
pin asserts only the message prefix plus the position suffix for this reason.

## 2. Implementation record

(to be filled in steps 4–5: module, seams, position search, TVF/REQUIRES shapes.)

## 3. C-006 retirement record

(to be filled in step 6.)

### pins retired — announce to run 18a

(to be filled in step 6: each file + test.)

### names Spark has that the SQL door lacks

(to be filled in step 6.)

## 4. Hand-offs

- H-001 (P2 → run 18b): `selectExpr` fragment-relative `line 1 pos 0` needs a
  facade edit in `dataframe/core.py` (fenced here). Rust pins the class.
- H-002 (P2 → error-surface owner): engine `AnalysisException.getCondition()` /
  `getSqlState()` parsing the `[CLASS]` / `SQLSTATE:` out of the message.
- H-003 (note → run 18a): `functions_byname._resolve_routine` can delegate to
  the Rust path; also its dotted-name arm raises `REQUIRES_SINGLE_PART_NAMESPACE`
  for every dotted name while the SQL door answers `UNRESOLVED_ROUTINE` for
  `spark_catalog.default.x` and `nosuch.fn` — doors disagree there, no oracle
  cell pins it.

## 5. Gates

(to be filled in step 8: exact commands, exit codes, counts.)
