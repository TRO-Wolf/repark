# Charter ledger — LIT-DECIMAL-1 · `F.lit(Decimal(...))` and `F.like`'s escape argument

**Date:** 2026-09-15 · **Branch:** `feat/lit-decimal-1` · **Base:** `origin/main`
`0355ef5e` · **Model:** swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** candidate rows noted in §4 for step 2's C-008 pass.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** Two parity gaps the campaign found and never closed, scheduled to run 17a:
PySpark 4.1.2 accepts a Python `decimal.Decimal` in `F.lit` and types it with Spark's
decimal literal rules (DECIMAL-CACHE-1 side finding), and `F.like` / `F.ilike` take a
third `escapeChar` parameter (run 15c's P2). Oracle: 46 cells recorded on live PySpark
4.1.2 by the run-17a orchestrator, copied verbatim to
`python/repark/tests/lit_decimal_1_spark_oracle.json`.

**Not in this unit:** `typeof` as a callable name (absent engine-wide — the `typeof`
cells pin through the R2 schema-assert translation), folded-cast projection naming
(§4), `repark.sql()` native-door coverage, and any edit under the run-17b/17c fences.

## Orchestrator rulings (G-2), copied from the card

- D-1 Rust first. The decimal literal typing is a Rust literal path
  (`crates/repark-core` / `crates/repark-functions`, whichever already owns literal
  construction — find it, do not guess), and `like`/`ilike`'s escape argument binds an
  existing Rust kernel. The Python facade converts the `decimal.Decimal` to the
  engine's literal and does no precision/scale arithmetic of its own. If any part must
  stay in Python, state the engine limitation in one ledger line — that line goes in
  the run's Rust-first roll-call.
- D-2 `lit` is a frozen 1.0 name. Do not change its signature (`lit(col: Any)`); this
  is a widening of what `col` accepts, not a signature change.
- D-3 `like` / `ilike` gain an optional third parameter named exactly `escapeChar`,
  defaulting to `None` — PySpark 4.1.2's recorded signature. Adding an optional
  parameter to a frozen name is allowed (card D-2 precedent in FNP-11B); a required
  parameter may not move.
- D-4 Never edit `python/repark/src/repark/spark/dataframe/**`, `column.py`,
  `session/**`, `catalog.py`, `types.py` (run 17b) or the SQL parser/planner beyond
  function registration (run 17c). HALT with the exact seam instead.
- D-5 No dependency changes. No `Cargo.toml`, `Cargo.lock`, `pyproject.toml`,
  `uv.lock`, `.github/`.

**Ledger decisions (actor, 2026-09-15):**

- D-6 `LIT-DS-*` error class: Spark's recorded class is a Py4J-transported
  `java.lang.NumberFormatException` — RePark has no Py4J boundary and no such class,
  so the nearest honest refusal is an engine/`PySparkException` error carrying that
  message's text shape. The pins assert the recorded
  `java.lang.NumberFormatException:` line verbatim (`Character <c> is neither a
  decimal digit number, decimal point, nor "e" notation exponential mark.`).

## PROPOSITION LEDGER — LIT-DECIMAL-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `F.lit(Decimal)` types every `LIT-D-*`, `LIT-DT-00`, `LIT-DI-00` cell exactly (dtype, nullability, value). | `test_lit_decimal_1.py::test_lit_decimal_cells`, 17 cells. | OPEN | Red at step 1: all 17 cells raise `PySparkTypeError` ("lit() supports … got Decimal") on the unfixed tree. |
| C-002 | A Decimal past 38 digits (and the 39-digit SQL literal) raises `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION` with Spark's text. | `test_lit_decimal_over_precision_raises` (LIT-D-12/14/16, LIT-DF-00) + `test_sql_decimal_literal_over_precision_raises` (LIT-SQL-04). | OPEN | Red at step 1: 5 cells — facade cells hit the `lit()` refusal; LIT-SQL-04 silently answers `decimal(39,0)`. |
| C-003 | `LIT-DS-*` non-finite Decimals raise with the recorded `NumberFormatException` text shape (D-6). | `test_lit_decimal_non_finite_raises`, 4 cells. | OPEN | Red at step 1: all 4 cells hit the `lit()` refusal instead of the recorded text shape. |
| C-004 | Decimal-literal arithmetic and the SQL-door cells agree with `LIT-A-*` / `LIT-SQL-*`. | `test_lit_decimal_arith_cells` (4) + `test_sql_decimal_literal_cells` (7 ok cells; `typeof` translated to schema asserts). | OPEN | Red at step 1 on the 4 arith cells (lit refusal). **Already-green controls:** LIT-SQL-00, 01, 02, 03, 05, 06, 07 — 7 cells pass on the unfixed tree. |
| C-005 | `like`/`ilike` accept `escapeChar` and answer `LIKE-00…03` plus the `LIKE-SQL-*` cells. | `test_like_ilike_signatures_carry_escape_char`, `test_like_escape_cells` (4), `test_sql_like_escape_cells` (3). | OPEN | Red at step 1: signature pin + 3 facade cells (`TypeError`, frozen 2-arg) + 3 SQL cells (kernel/arity). **Control:** LIKE-02 (two-arg `like`) already green. |
| C-006 | A bad escape (`!!`, ``) raises `INVALID_ESCAPE_CHAR` with Spark's text. | `test_like_escape_bad_escape_raises` (LIKE-04, LIKE-05). | OPEN | Red at step 1: 2 cells hit the frozen-signature `TypeError`. |
| C-007 | No regression and census pins. | Neighbor suites (`test_decimal_cache_1.py`, `test_fn_like_escape_end.py`, `test_functions_*`) stay green; `make verify` at the build clone in step 2. | OPEN | Step-2 gate. |
| C-008 | Registry rows and maps in lockstep. | `docs/spark-sql-iceberg-parity.md` rows for the measured residuals (§4); `map.md` files updated in the same commits. | OPEN | Step-2 gate; step-1 map rows landed with the pin commit. |

VERDICT: 8 clauses, 0 PROVEN, 8 OPEN, 0 REJECTED.

## 1. Red-first record (base `0355ef5e`, before any product edit)

`.venv/bin/python -m pytest python/repark/tests/test_lit_decimal_1.py -q` on the base
tree: **39 failed, 8 passed** (47 tests = 46 oracle cells + 1 signature pin).
Representative failure modes:

- Every facade `F.lit(Decimal)` cell (29 tests across C-001/C-002/C-003/C-004):
  `PySparkTypeError: lit() supports None, bool, int, float, str, date, datetime,
  time, list, tuple, ndarray, or Enum; got Decimal`.
- Facade `like`/`ilike` three-arg cells (5 tests): `TypeError: like() takes 2
  positional arguments but 3 were given` (and the `ilike` twin); the signature pin
  finds no `escapeChar` parameter.
- SQL-door `LIKE … ESCAPE '!'` (LIKE-SQL-00/01): `PySparkException: datafusion
  engine error: Execution error: LIKE does not support escape_char other than the
  backslash (\)` — the card's "already answers" control claim is measured wrong;
  DataFusion's kernel refuses a non-`\` escape at execution.
- SQL-door `like('a_b','a!_b','!')` (LIKE-SQL-02): `AnalysisException: Function
  'like' expects 2 arguments but received 3`.
- `SELECT 123456789012345678901234567890123456789` (LIT-SQL-04): answers
  `decimal(39,0)` instead of raising `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION`.

**Already-green controls (8):** LIT-SQL-00, LIT-SQL-01, LIT-SQL-06 (value + type;
see §4 for the projection name), the four `typeof` cells LIT-SQL-02/03/05/07
through the schema-assert translation, and LIKE-02 (two-arg `like`).

## 2. Work log

- 2026-09-15 step 1 (thin clone `/tmp/ra-lit-1`, no build): ledger created; oracle
  copied verbatim to `python/repark/tests/lit_decimal_1_spark_oracle.json`; red pins
  in `python/repark/tests/test_lit_decimal_1.py` — one parametrized pin per cell
  group, cell ids as test ids, both doors where the cell carries a SQL form.
  `typeof` cells translate to `SELECT <inner>` + field-type assert (the R2
  precedent; `typeof` is not an engine name — §4). Seam notes for step 2 in §3.

## 3. Seam notes for step 2 (build clone)

- Facade `lit`: `python/repark/src/repark/spark/functions.py::lit` refuses
  non-scalars at its isinstance gate. The native scalar path is
  `PyColumn::literal` at `crates/repark-python/src/column/mod.rs:84`
  (None/bool/int/float/str only). The typed-literal precedent that also produces
  the display + sql fragments is the `PyColumnParts.lit_*` family —
  `crates/repark-python/src/column/display.rs` delegating to
  `crates/repark-python/src/column/display/construct.rs` (`lit_timestamp`,
  `lit_date`, `lit_time`, `lit_array_cast`). A `lit_decimal` sibling taking the
  Decimal's canonical text (`str(value)` preserves digits, exponent, trailing
  zeros and the 55-digit float expansion) and producing `ScalarValue::Decimal128`
  with Spark's digit/exponent precision-scale rules plus the >38
  `DECIMAL_PRECISION_EXCEEDS_MAX_PRECISION` refusal is the D-1 Rust home.
- `like`/`ilike`: `functions_expr.py` → `_scalar` → `PyColumn.call_scalar` →
  `crates/repark-python/src/column/function_dispatch.rs` arms `"like"`/`"ilike"`
  (currently `need(2)`, `expr.like(pattern)` → `Expr::Like` with
  `escape_char: None`). The analyzer already reads `like.escape_char`
  (`crates/repark-functions/src/analyzer/like_escape.rs`, ESC_AT_THE_END); the
  `INVALID_ESCAPE_CHAR` length-one check joins that site. The non-`\` escape needs
  a Spark-semantics path — DataFusion's kernel refuses it at execution (measured
  red), so either a pattern/escape translation or a repark kernel; the SQL-door
  `like(a,b,esc)` call needs a three-arg function registration (allowed — D-4
  fences the parser/planner *beyond* function registration).
- SQL decimal literal: plain `1.5` flows through DataFusion's own literal typing
  (already `decimal(2,1)`); `BD` suffixes canonicalize to
  `CAST(__repark_suffix_literal__(N) AS DECIMAL(p,s))` in
  `crates/repark-spark/src/spark_literals.rs`. The 39-digit literal currently
  answers `decimal(39,0)` — the >38 gate has no home yet; an analyzer literal scan
  is the candidate (repark-functions analyzer chain).

## 4. Measured residuals for step 2's registry pass (C-008)

- `typeof` is not a resolvable name on either door (`Invalid function 'typeof'`);
  the four `typeof` cells pin through the schema-assert translation. The name
  itself is a separate surface gap (run 15a inventory lists it unimplemented).
- `SELECT CAST(1.5 AS DECIMAL(10,4))` answers `decimal(10,4)` / `1.5000` but names
  the field `1.5000` where Spark records `CAST(1.5 AS DECIMAL(10,4))` — the
  folded-literal display name diverges; value and type pinned.
- `SELECT <39 digits>` answers `decimal(39,0)` — no precision gate on SQL literals.

## 5. Gates

- Step 1 (thin clone): pin file red run — 39 failed / 8 passed (§1). No Rust gates
  here (no build allowed); `make verify` and the neighbor suites run at the build
  clone in step 2.
