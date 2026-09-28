# Unit ledger — WO INTDIV-1 · arithmetic over integer `/` keeps Spark's DOUBLE in every scope

**Date:** 2026-09-28 · **Branch:** `fix/derived-int-division` · **Base:** `7bbb8e95` (`origin/main`)
**Model:** Claude (`claude-opus-5-5`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** A v1.5.1 verifier found `SELECT h * 2 FROM (SELECT id / 2 AS h FROM t)`
answering BIGINT `0, 2, 2` on RePark where Spark answers DOUBLE `1.0, 2.0, 3.0` (`/` is always
fractional in Spark). Present on `main` and in the released v1.5.0.

**Diagnosis, re-measured.** The verifier's mechanism holds — something reads the type of `id / 2`
before `SparkExprSemantics` rewrites it to a DOUBLE division — but the measured surface is wider
than nesting, and the rule order alone is not the cause:

1. The integer `ExprPlanner` (`SparkIntegerExprPlanner`) arms `+ - *` at **SQL plan time**, when
   DataFusion still types `id / 2` as BIGINT. It wraps both operands in truncating
   `CAST(… AS BIGINT)`s, so the same-scope `id / 2 + 1` is also wrong on `main` (BIGINT
   `1, 2, 2`), and a derived-table column `h` is armed from its plan-time schema. Reordering the
   analyzer rules cannot undo a plan-time cast.
2. `SparkIntegerOverflow` (analyzer) arms from the same stale types for plans the planner did
   not see.
3. The first `TypeCoercion` bakes casts from the stale type too: `sum(h)` over `h = i / 2`
   (INT quotient) receives `CAST(h AS BIGINT)` and answers BIGINT `2` where Spark answers DOUBLE
   `3.0`.

**Fix.** One new pre-coercion analyzer rule, `SparkFractionalDivision`
(`crates/repark-functions/src/integer_spark/fractional_division.rs`), seated first among the
`analyzer_rules_with_higher_order_preparation` inserts. It walks every plan node bottom-up
(with subqueries, recomputing each schema), so a scope's column is DOUBLE before its parent is
read: an integer `/` integer division becomes `CAST(l AS DOUBLE) / CAST(r AS DOUBLE)` (the
shape `SparkExprSemantics` already produced), and a checked integer call whose cast-wrapped
operand turned fractional is unarmed back to the plain operator for the following
`TypeCoercion`. Plans without `/` are untouched. The ANSI (Trino) door never seats it.
About 130 product lines (the non-test part of the rule module) plus the seat lines in `lambda_rebind.rs` and one module line in
`integer_spark.rs`.

**What Spark does (Spark 4.1.2 + Iceberg 1.11.0, `local[1]`, ANSI default on, UTC, hadoop
catalog; probe `/tmp/oc-worker/direct/wo/intdiv-probes/intdiv_probe.py`, outputs
`out/intdiv-{spark,repark,repark-base}.json`; the replayed subset is committed as
`python/repark/tests/intdiv_1_spark_oracle.json`).** Table `sc.ns.t (id BIGINT, i INT, d
DECIMAL(10,2))` rows `(1,1,1.00), (2,2,2.00), (3,3,3.00)`; `h = id / 2` or `i / 2`:

| Cell | Spark |
|---|---|
| `h`, `-h`, `abs(h)` | double `0.5, 1.0, 1.5` (negated for `-h`) |
| `h * 2` / `h + 1` / `h - 1` | double `1.0, 2.0, 3.0` / `1.5, 2.0, 2.5` / `-0.5, 0.0, 0.5` |
| `h / 2`, `h % 2` | double `0.25, 0.5, 0.75`; double `0.5, 1.0, 1.5` |
| `CAST(h AS INT)` | int `0, 1, 1` |
| `WHERE h + 1 = 1.5` | keeps `id = 1` |
| `ORDER BY h + 0 DESC` | ids `3, 2, 1` |
| `sum(h)` / `avg(h)` | double `3.0` / double `1.0` |
| same answers in the same scope, derived table, CTE, temp view, session-catalog view, nested derived tables | — |
| `g + 1` over `g = h * 2` over `h = id / 2` | double `2.0, 3.0, 4.0` |
| `h + 9223372036854775807` | double `9.223372036854776e18` ×3, no error |
| `a + 1` over `a = id * 2` / `a = i * 2` | bigint / int `3, 5, 7` |
| `a + 1` over `a = 9223372036854775807` (derived, CTE) | `ARITHMETIC_OVERFLOW` long overflow, `try_add`, 22003 |
| `a * 2147483647` over `a = i * 2` | `ARITHMETIC_OVERFLOW` integer overflow, `try_multiply`, 22003 |
| CTAS `h * 2 AS v, h + 1 AS w` | `DESCRIBE` double/double; values `(1.0,1.5), (2.0,2.0), (3.0,2.5)` |
| INSERT the same into `(v DOUBLE, w DOUBLE)` / `(v BIGINT, w BIGINT)` | `(1.0,1.5), (2.0,2.0), (3.0,2.5)` / `(1,1), (2,2), (3,2)` |
| DataFrame `select(col("h") * 2)`, `selectExpr("h + 1")`, `filter("h + 1 = 1.5")`, `(col("i") / 2 + 1)`, `agg(sum("h"))` over `i / 2` | double, as the SQL cells |

**Measured on main (RePark, same probe).** 50 cells differed outside the decimal, `div` and
view-DDL families: every `* + -` over `h` in the same scope, derived table, CTE and nested
scopes answered BIGINT or INT with truncated values; `WHERE h + 1 = 1.5` kept no row;
`ORDER BY h + 0` sorted `2, 3, 1`; `sum(h)` over the INT quotient answered bigint `2`; the chain
answered bigint `1, 3, 3`; `h + 9223372036854775807` raised `ARITHMETIC_OVERFLOW`; CTAS stored
BIGINT `(0,1), (2,2), (2,2)`; INSERT into DOUBLE stored `0.0, 2.0, 2.0`. Temp-view and
session-view cells and the DataFrame door were already right (their plans are analyzed before
the outer arithmetic is planned). After the fix the same diff is 4 cells, all residues below.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Arithmetic over an integer `/` answers Spark's DOUBLE type and values in the same scope, a derived table, a CTE, a temp view, a session-catalog view, nested derived tables and a chained derived scope, over BIGINT and INT operands; `WHERE` and `ORDER BY` over it see the fraction; `sum`/`avg` over it are DOUBLE; `h + 9223372036854775807` does not overflow; CTAS stores DOUBLE and INSERT into DOUBLE columns stores Spark's values. | Rust `integer_spark::fractional_division::tests` (seven value/type pins on the production analyzer order) and `test_intdiv_1.py` (every oracle cell per scope group, the store cells and the DataFrame door). | PROVEN | `cargo test -p repark-functions --lib fractional_division`: 8 passed. M1 (rule unseated, `main` behaviour): seven Rust pins red, facade 12 of 17 red (the view groups and the DataFrame door were already right). Facade file: 17 passed. |
| C-002 | The fix is one pre-coercion rule seated first among the `analyzer_rules_with_higher_order_preparation` inserts, immediately before `higher_order_preparation`; the other four seats keep their offsets; a plan without `/` is returned untouched; the ANSI door does not seat it. | The seat contract test in `crates/repark-spark/src/extension/tests.rs`; M2 and M3 show each half of the rule is load-bearing. | PROVEN | `analyzer_configuration_seats_hof_preparation_and_float_stringify_before_type_coercion` asserts the new seat and passes; the full `repark-sql --tests` (ANSI door) run is green. M2 (no unarm): six Rust pins red, `sum` stays green (it needs only the division half). M3 (no division rewrite): seven red. |
| C-003 | Genuinely integral arithmetic is unchanged: derived BIGINT and INT arithmetic keeps its type and values, and `ARITHMETIC_OVERFLOW` under ANSI still raises for BIGINT and INT overflow through a derived table or CTE. | Rust `integral_derived_arithmetic_stays_integral` and the facade `ctl` group, plus the full F-Y10-1 overflow battery in `integer_spark.rs`. | PROVEN | Green before and after the fix and under M1–M3 (a control, not a red pin); `cargo test -p repark-functions --lib` 880 passed; the facade `ctl` group replays Spark's condition and message head. |

## Mutation record (2026-09-28)

| # | Mutation | Red |
|---|---|---|
| M1 | Unseat `SparkFractionalDivision` (restore `lambda_rebind.rs` to `main`) | Seven of eight Rust pins red (all but the integral control); facade `test_intdiv_1.py` 12 of 17 red; restored green |
| M2 | Keep the division rewrite, skip the unarm branch | Six Rust pins red (`sum_over_int_division_is_double` and the control green); restored green |
| M3 | Keep the unarm branch, skip the division rewrite | Seven Rust pins red (control green); restored green |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: intdiv-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: C-001 is walked cell by cell against the recorded Spark answers in every scope family, over BIGINT and INT, with names, types and rows compared exactly.
      artifacts: [crates/repark-functions/src/integer_spark/fractional_division.rs, python/repark/tests/test_intdiv_1.py, python/repark/tests/intdiv_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: Same scope, derived table, CTE, temp view, session view, nested and chained scopes; filter, sort, aggregate, CTAS and INSERT consumers; BIGINT MAX plus a fraction; lambda-variable divisions are skipped and stay with SparkExprSemantics; non-integer and decimal divisions are not touched.
      artifacts: [crates/repark-functions/src/integer_spark/fractional_division.rs, python/repark/tests/test_intdiv_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: The rule adds no error path; an operand whose type cannot be read is left for the existing post-coercion rewrite. The INSERT-into-BIGINT internal error that main already raised for a same-scope division now also covers the derived-arithmetic shape that main silently mis-stored (R-INTDIV-1).
      artifacts: [crates/repark-functions/src/integer_spark/fractional_division.rs]
    - id: AT-4
      status: N/A
      justification: Stateless analyzer rule; no shared or mutable state.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling.
    - id: AT-6
      status: ATTACKED
      evidence: CTAS now stores DOUBLE and INSERT into DOUBLE stores Spark's values where main stored truncated integers; the stored-type and read-back cells are pinned.
      artifacts: [python/repark/tests/test_intdiv_1.py]
    - id: AT-7
      status: N/A
      justification: No performance claim; plans without `/` skip the rule after one expression scan, and plans with `/` pay one extra bottom-up rewrite pass.
    - id: AT-8
      status: ATTACKED
      evidence: No new dependency or crate edge; the rule is a child module of integer_spark reached through the existing pre-coercion seat function; the ANSI door, which seats only the overflow rule, is unchanged and its suite is green.
      artifacts: [crates/repark-functions/src/lambda_rebind.rs, crates/repark-functions/src/integer_spark.rs, crates/repark-spark/src/extension/tests.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The integral overflow refusals keep Spark's condition and message head through a derived table and a CTE; the facade compares both.
      artifacts: [crates/repark-functions/src/integer_spark/fractional_division.rs, python/repark/tests/test_intdiv_1.py]
    - id: AT-10
      status: ATTACKED
      evidence: M1 unseats the rule and the value pins red; M2 and M3 remove each half and pins red, proving both branches are load-bearing; the integral control stays green under every mutation.
      artifacts: [crates/repark-functions/src/integer_spark/fractional_division.rs, python/repark/tests/test_intdiv_1.py]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-INTDIV-1 | Dated 2026-09-28 (pre-existing, widened): `INSERT INTO <BIGINT table> SELECT id / 2 …` fails on `main` with `Internal error: cannot convert text Float64 into byte Int64`, because DataFusion's INSERT planning chose the store cast from the plan-time BIGINT type of `/`. Spark stores `1, 2, 3` / `1, 2, 2` (measured `store/ins_big_read`). With this unit the derived shape `INSERT … SELECT h * 2, h + 1 FROM (SELECT id / 2 AS h …)`, which `main` silently stored as `(0,1), (2,2), (2,2)`, fails the same way instead. |
| R-INTDIV-2 | Dated 2026-09-28 (pre-existing): `d / 2` over `DECIMAL(10,2)` is `decimal(14,6)` on Spark (the literal `2` is `DECIMAL(1,0)`) and `decimal(21,13)` on RePark in every scope, and every expression and aggregate over it inherits the wider type (values equal). The session-view `h / 2` cell answers `decimal(25,17)` against `decimal(32,24)` in the other RePark scopes. |
| R-INTDIV-3 | Dated 2026-09-28 (pre-existing): the `div` operator does not parse (`ParserError`); Spark answers bigint for integral and decimal operands and `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` for a DOUBLE operand. |
| R-INTDIV-4 | Dated 2026-09-28 (pre-existing): the facade `ARITHMETIC_OVERFLOW` refusal carries no SQLSTATE (Spark `22003`); the pin compares condition and message head only. |
| R-INTDIV-5 | Dated 2026-09-28 (pre-existing, integral, out of this unit by instruction): `SELECT a + 1 FROM (SELECT 2147483647 AS a) s` widens to bigint `2147483648`; Spark raises `ARITHMETIC_OVERFLOW` integer overflow (the derived literal is typed INT on Spark and BIGINT at RePark plan time). |
| R-INTDIV-6 | Dated 2026-09-28 (pre-existing): `CREATE VIEW sc.ns.cv_* …` on the hadoop Iceberg catalog succeeds on RePark; Spark refuses with `UnsupportedOperationException: Creating a view is not supported by catalog: sc`. The catalog-view scope is therefore measured through the session catalog. |
| R-INTDIV-7 | Dated 2026-09-28 (pre-existing): `typeof(v)` over a table column is named `typeof(sc.ns.c1.v)` on RePark and `typeof(v)` on Spark; DDL/DML statements answer `[[]]` / a count row where Spark answers no rows. |
