# Unit ledger — WO CAST-OVERFLOW-INSERT-1 · out-of-range fractional stores refuse `CAST_OVERFLOW_IN_TABLE_INSERT` like Spark on every door

**Date:** 2026-09-29 · **Branch:** `fix/cast-overflow-insert-1` · **Base:** `5fb38051` (`origin/main`)
**Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** [R-INTDIV-9](../staging/intdiv-1-ledger.md) holds that an out-of-range DOUBLE,
NaN or Infinity stored into an integer column refuses on both engines but with different
text: Spark answers `CAST_OVERFLOW_IN_TABLE_INSERT` (SQLSTATE 22003, source type, target
type and column named) while RePark answers `Optimizer rule 'simplify_expressions'
failed`, a raw Arrow `Can't cast value` error, or `DIVIDE_BY_ZERO` for `0/0`. This unit
closes that residue on every write door.

**Fix.** `StoreOverflowCast`, a new analyzer rule over `LogicalPlan::Dml`
(`crates/repark-iceberg/src/write/store_overflow.rs`), seated last among the Spark-door
analyzer rules so it consumes `SparkExprSemantics`' guarded-division shape. Constants
fold at plan through `store_fold.rs` (which substitutes subquery and temp-view lineage
before evaluating, so an empty source still refuses); column expressions are wrapped in
the Spark-boundary checked-cast UDFs from the rewritten `store_cast.rs`; zero-divisor
guards are swapped for the store guard through the defining projections, so `0/0` and
`1/0.0` name the division type instead of raising `DIVIDE_BY_ZERO`. `wrap_store_outputs`
is the same conformance for the non-Dml plans (MERGE arms, the UPDATE scratch rewrite,
OVERWRITE and BY NAME sources), applied between eager analysis and optimization. Every
refusal is the execution-shaped error, so the facade renders Spark's bare
`ArithmeticException` text on every door. About 2100 product lines (the rule, fold and
kernel modules plus their unit tests) and 240 end-to-end lines, driven by per-door
plan-shape handling; the two file-size splits (`store_fold.rs`,
`insert_overwrite/store.rs`) are pure moves.

**What Spark does (Spark 4.1.2 + Iceberg 1.11.0, `local[1]`, ANSI on, UTC, hadoop
catalog; probe `/tmp/gen_cov_oracle2.py`, Spark output `/tmp/cov_oracle_spark.json`;
the replayed cells are committed as
`python/repark/tests/cast_overflow_insert_1_spark_oracle.json`).** Tables `sc.ns.cov
(id BIGINT, v BIGINT)` rows `(1,0),(2,0)`, `covi` (INT), `covtiny`/`covsmall`
(Iceberg widens TINYINT/SMALLINT to INT — measured `DESCRIBE v int` on Spark),
`srcd` (DOUBLE), `srcf` (FLOAT), `srce` (empty), `srcz`:

| Cell | Spark |
|---|---|
| `1e19`, `-1e19`, `NaN`, `Infinity`, `-Infinity` into BIGINT on VALUES, INSERT SELECT, OVERWRITE, BY NAME, UPDATE, MERGE both arms, `writeTo`, `insertInto` | `CAST_OVERFLOW_IN_TABLE_INSERT`, DOUBLE→BIGINT, column `` `v` ``, 22003, `ArithmeticException`, nothing written |
| `1e10`, `2147483648.0`, `3.5e9` float into INT | same class, DOUBLE→INT, 22003 |
| `0/0` on every door | same class, DOUBLE→BIGINT, 22003 |
| `1/0.0` on every door | same class, DECIMAL(8,6)→BIGINT, 22003 |
| `(x-x)/(x-x)` column division on every door | same class, DOUBLE→BIGINT, 22003 |
| `CAST(1e30 AS DECIMAL(38,0))`, `2e18` decimal into BIGINT | same class, DECIMAL(38,0)→BIGINT, 22003 |
| const overflow over an empty source (SELECT, BY NAME, OVERWRITE, writeTo) | same class (the store is validated with no rows read) |
| `42.0`, `1.5`, `-1.5`, `CAST(12 AS DECIMAL)`, `9.223372036854776e18`, `-9.223372036854776e18` | stored truncated toward zero (`9.223372036854776e18` stores MAX) |
| `300.0` into TINYINT, `40000.0` into SMALLINT, `300.0` into SMALLINT | stored: the columns are Iceberg `int`, so the values fit |
| `try_cast(1e19 AS BIGINT)` | stored NULL |
| `UPDATE … SET v = 1e19 WHERE false` | succeeds, nothing written |
| `INSERT … SELECT 'abc'`, `UPDATE … SET v = 'abc'` | `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST` refuses |
| `SELECT 0/0`, `INSERT … SELECT 0/0` into a DOUBLE column | `DIVIDE_BY_ZERO` refuses |
| `2147483647 + 1` into BIGINT | `ARITHMETIC_OVERFLOW` refuses, 22003 |

**Measured on main (RePark, same probe).** 39 of 76 cells differed: every overflow cell
refused with the rule-context head, the optimizer failure, the raw Arrow cast text, or
`DIVIDE_BY_ZERO`; empty-source SELECT/OVERWRITE/writeTo succeeded or wiped-guarded;
`UPDATE … WHERE false` failed the optimizer. After the fix the same diff is 13 cells:
4 decimal-naming divergences, the empty-overwrite wipe text, the pre-existing UPDATE
common-expr failure, the pre-existing CTE gap, the brief-residue int-overflow text, 4
unchanged must-not-change controls, and one tie-order artifact.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Every out-of-range DOUBLE, FLOAT or DECIMAL store into an integer column — `1e19`, `-1e19`, NaN, ±Infinity, `1e10`/`2e18` into INT, `CAST(1e30 AS DECIMAL(38,0))`, `0/0`, `1/0.0`, column and subquery/view division — refuses with Spark's `CAST_OVERFLOW_IN_TABLE_INSERT` (SQLSTATE 22003, source type, target type and column named, `ArithmeticException`) and writes nothing, on VALUES, INSERT SELECT, INSERT OVERWRITE, BY NAME, UPDATE SET, MERGE UPDATE, MERGE INSERT, `writeTo().append()` and `insertInto`, from constant and column sources. | Facade `test_cast_overflow_insert_1.py` (every oracle cell per door group, refusal err/head/state plus read-back proof) and Rust `cast_overflow_insert.rs` (seven end-to-end doors over a real Iceberg table). | PROVEN | Facade file: 9 passed (76 cells). Rust `tests::cast_overflow_insert`: 7 passed. M1 (doubled float boundary) reds 7 of 9 facade groups; restored green. |
| C-002 | In-range fractional values store truncated toward zero (`42.0`, `1.5`, `-1.5`, decimals, `9.223372036854776e18` storing MAX); `try_cast` overflow stores NULL; `UPDATE … WHERE false` succeeds; `300.0`/`40000.0` store into the Iceberg-widened INT columns. | The store cells of the facade file (rows and column types compared) and the Rust in-range test. | PROVEN | Same runs as C-001, all green; the widened `int` column types are asserted, so a future byte-preserving Iceberg re-reds the pins. |
| C-003 | Integer-to-integer overflow, string refusals and plain `DIVIDE_BY_ZERO` keep their text: `2147483647 + 1` still raises `ARITHMETIC_OVERFLOW`, `'abc'` stores still refuse without the overflow class, and `0/0` outside a store still raises `DIVIDE_BY_ZERO`. | Facade keep-pins plus a 25-statement neighbour battery and 9 triage cells run on base and head. | PROVEN | Neighbours: 25 of 25 identical base-vs-head; triage: 8 of 9 identical, the one flip (`UPDATE … WHERE false` optimizer error → success) matches Spark. The gate's `test_intdiv_1.py` and `test_ltz_store_int_1.py` stay green. |
| C-004 | The fix is one Dml analyzer rule seated after the post-coercion rules plus the same conformance applied between eager analysis and optimization on the four non-Dml internal plans; no new crate edge; plans without a fractional-to-integer store are returned untouched. | Crate-DAG gate, the rule's no-op paths, and the neighbour battery. | PROVEN | `crate-dag` clean; `cargo test -p repark-spark --lib` green; neighbours prove untouched shapes (nulls, identity updates, CTAS, self-overwrite, reorder) answer identically. |

## Mutation record (2026-09-29)

| # | Mutation | Red |
|---|---|---|
| M1 | Double the float upper boundary in `check_float_store_value` (`bounds.1 * 2.0`), so `1e19` stores | Facade `test_cast_overflow_insert_1.py` 7 of 9 red (values, insel, ovw, byname, upd, merge, dataframe); the controls and range groups stay green (no float-boundary cell); restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cast-overflow-insert-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every brief source (1e19, -1e19, NaN, infinities, 1e10 into INT, 1e30 decimal, 0/0, 1/0.0, column division) is walked against the recorded Spark answers on all nine doors, comparing err, message head, SQLSTATE and read-back rows.
      artifacts: [python/repark/tests/test_cast_overflow_insert_1.py, python/repark/tests/cast_overflow_insert_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Constant, column, subquery, temp-view and empty sources; boundary values (MAX double, MAX/MIN int, try_cast, WHERE-false); INT and BIGINT targets plus the Iceberg-widened TINYINT/SMALLINT behavior; narrow kernels pinned at unit level.
      artifacts: [crates/repark-iceberg/src/write/store_cast.rs, crates/repark-spark/src/tests/cast_overflow_insert.rs, python/repark/tests/test_cast_overflow_insert_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The rule returns non-Dml plans untouched and skips pairs outside float/decimal-to-int; int-to-int, string and divide-by-zero refusals keep their text by keep-pin and neighbour battery; the UPDATE empty-match stays lazy.
      artifacts: [crates/repark-iceberg/src/write/store_overflow.rs, python/repark/tests/test_cast_overflow_insert_1.py]
    - id: AT-4
      status: N/A
      justification: Stateless analyzer rule and pure plan rewrites; no shared or mutable state.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling.
    - id: AT-6
      status: ATTACKED
      evidence: Every refusal cell asserts the post-statement read-back equals the pre-statement rows; every store cell asserts exact rows and column types.
      artifacts: [python/repark/tests/test_cast_overflow_insert_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; plans without a fractional-to-integer store skip the rule after one scan, and const folding evaluates only store-bound constants.
    - id: AT-8
      status: ATTACKED
      evidence: No new crate edge; new modules are children of existing ones; the two file-size splits are pure moves; the ANSI door suite is green.
      artifacts: [crates/repark-iceberg/src/write/store_fold.rs, crates/repark-spark/src/insert_overwrite/store.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Integer-overflow, string and divide-by-zero refusals keep Spark's condition and message head by keep-pin; the facade compares both where the engine parses them.
      artifacts: [python/repark/tests/test_cast_overflow_insert_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: M1 doubles the float boundary and the value pins red across seven door groups while the controls stay green, proving the pins judge values, not shape.
      artifacts: [crates/repark-iceberg/src/write/store_cast.rs, python/repark/tests/test_cast_overflow_insert_1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-COV-1 | Dated 2026-09-29 (decimal inference, out of scope): RePark types `1/0.0` as `DECIMAL(17,6)` on the Python door (`DECIMAL(27,6)` in the Rust e2e session); Spark types `DECIMAL(8,6)`. The four `dec-div0` pins assert RePark's head with the Spark head beside it in the oracle. Fixing decimal-division inference is a separate unit. |
| R-COV-2 | Dated 2026-09-29 (pre-existing, fails identically at base): `UPDATE … SET v = (id - id) / (id - id)` fails with `Schema error: No field named __common_expr_1` before any store check runs. Column arithmetic in UPDATE SET breaks common-expr lineage in the UPDATE planner; pinned as a divergence. |
| R-COV-3 | Dated 2026-09-29 (pre-existing, both refuse): `INSERT OVERWRITE … SELECT 1e19 FROM <empty>` refuses with the O4-C2-Q-001 wipe-guard text instead of Spark's `CAST_OVERFLOW`; the guard type-checks before any value check. Narrowing it needs the guard cluster moved for value-level preemption; pinned as a divergence. |
| R-COV-4 | Dated 2026-09-29 (pre-existing, fails identically at base): `INSERT` with a `WITH` subquery answers `not implemented yet`. Pinned as a keep-pin so implementing it must replace the pin. |
| R-COV-5 | Dated 2026-09-29 (brief residue, unchanged by base-vs-head triage): `2147483647 + 1` integer-arithmetic overflow keeps its `ARITHMETIC_OVERFLOW … (ArithmeticException)` text against Spark's `SQLSTATE: 22003` tail. The brief forbids fixing it here; pinned as a keep-pin. |
| R-COV-6 | Dated 2026-09-29 (brief premise, measured false on live Spark 4.1.2): TINYINT and SMALLINT Iceberg columns do not exist — `CREATE TABLE (v TINYINT) USING iceberg` widens to `int` on both engines, so `300.0` into TINYINT and `40000.0` into SMALLINT store (both engines agree). End-to-end narrow-target refusal is therefore untestable; the `__repark_store_int8/16__` boundary kernels are pinned at unit level and the widened stores are pinned by rows and column types. |
| R-INTDIV-9 | **CLOSED 2026-09-29 by this unit.** Was: out-of-range DOUBLE, NaN and Infinity refused with `simplify_expressions`, raw Arrow cast, or `DIVIDE_BY_ZERO` text instead of Spark's `CAST_OVERFLOW_IN_TABLE_INSERT`. Every named shape now refuses with Spark's class, SQLSTATE 22003, source/target/column naming and bare `ArithmeticException` text on all nine doors (C-001). |
| VALUES-CONSTANT-FOLD-PARITY | Dated 2026-09-29 (DIFF-PROBE clean, `/tmp/muse-worker/xovf/20260929T151909Z/handback.json`): 13 changed cells are near-misses, equally wrong at base — 10 VALUES-door divide-by-zero cells (`values/{tinyint,smallint,int}/div0`, `div00`, `div0i` plus `values/bigint/div0`; base `DIVIDE_BY_ZERO`, head `CAST_OVERFLOW_IN_TABLE_INSERT`, Spark `INVALID_INLINE_TABLE`), 2 out-of-range integer literals (`values/bigint/tmax1`, `tmin1`; base simplify-fail, head `CAST_OVERFLOW_IN_TABLE_INSERT`, Spark analysis-folded `CAST_OVERFLOW`), 1 CASE branch (`shape/int/case`; base Arrow out-of-range, head `CAST_OVERFLOW_IN_TABLE_INSERT`, Spark folded `CAST_OVERFLOW`). Matching Spark needs VALUES constant-fold plus inline-table analysis parity, a separate unit. Fixture trim same day: the oracle keeps the asserted cells only (dropped `meta`, `ordered`, `divergence`, `keep_pin`, none read by the test module; one cell per line, 1601 down to 104 lines, all 9 tests still green with no assertion changed); the full corpus is preserved at `/tmp/oc-worker/direct/wo/cast-overflow-evidence/cast_overflow_insert_1_spark_oracle.full.json`. |
