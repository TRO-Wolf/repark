# Unit ledger — LOG1P-1 · precise `log1p` / `expm1` kernels

**Retires:** this ledger moves to `../completed/` in the unit's last commit.
This file closes when LOG1P-1 merges, or when the owner closes the slate row.

**Unit:** LOG1P-1 · **Date:** 2026-09-02 · **Executor:** Grok (grok-4.6), Actor ·
**Branch:** `fix/log1p-expm1-precision` · **Base:** `a0fe83a`
**Model:** grok-4.6
**risk_tier:** standard.

Spark is the oracle. Tiny-arg cells match `StrictMath.log1p` / `StrictMath.expm1`
(`f64::ln_1p` / `f64::exp_m1`). Domain `log1p(x <= -1)` is NULL on live Spark SQL
(not `-Infinity` / `NaN`); pins follow Spark, not raw StrictMath.

## Proposition ledger

| ID | Clause | Proof obligation | Verdict |
|---|---|---|---|
| C-001 | Three-door pins at the measured grid (`1e-16`, `1e-10`, `-1e-10`, `1e-5`, `0`, `1`, `-1`, `-2`, `700`, `710`, NULL, NaN, INT, DECIMAL) assert live Spark values. Red today on facade tiny-args and missing SQL `log1p`. | Oracle table below; `test_log1p_1.py`. | **PROVEN** |
| C-002 | Two scalar kernels `f64::ln_1p` / `f64::exp_m1`, NULL-propagating, numeric coerce as `log`/`exp`; registered on both SQL doors; facade `_scalar` not composed. | `spark_log1p.rs`; `register_all` + `AnsiDialect` + native `PyReparkSession::native`; `functions_expr.py`. | **PROVEN** |
| C-003 | `F.log1p` and `F.expm1` join `docs/examples/functions/logs.py`; leave `backlog.txt`; `BACKLOG_BASELINE` 844 → 842. Coverage static and `--require-execute` exit 0. | Example + gate. | **PROVEN** |
| C-004 | Three-door cells plus a live cell; mutation restore-composed reds; `log`/`log2`/`ln`/`exp` and SEM-1 incidentals stay green. | `test_log1p_1.py`; mutation table. | **PROVEN** |
| C-005 | Registry BL-15 closed FIXED; `spark-function-parity.md` drops `log1p`/`expm1` from `PY_COMPOSED`; maps lockstep. No new log1p registry row. | Registry + design table + maps. | **PROVEN** |

## Oracle (live PySpark 4.1.2, 2026-09-02, JDK 17, `spark.sql.ansi.enabled=true`)

Arrow type `double` throughout. Facade `F.*` equals Spark SQL.

| Call | Spark | repark before |
|---|---|---|
| `log1p(1e-16)` | `1e-16` | facade `0.0`; SQL doors `Invalid function 'log1p'` |
| `log1p(1e-10)` | `9.999999999500001e-11` | facade `1.000000082690371e-10` |
| `log1p(-1e-10)` | `-1.00000000005e-10` | facade `-1.000000082790371e-10` |
| `log1p(1e-5)` | `9.999950000333332e-06` | facade `9.999950000398841e-06` |
| `log1p(0.0)` / `log1p(1.0)` | `0.0` / `0.6931471805599453` | same |
| `log1p(700.0)` / `log1p(710.0)` | `6.55250788703459` / `6.566672429803241` | same |
| `log1p(-1.0)` / `log1p(-2.0)` | NULL / NULL | facade NULL / NULL (via composed `log`) |
| `log1p(NULL)` / `log1p(NaN)` | NULL / nan | SQL doors unresolved |
| `log1p(CAST(0 AS INT))` / `log1p(1)` | `0.0` / `0.6931471805599453` | SQL doors unresolved |
| `log1p(DECIMAL 1e-16)` | `1e-16` | SQL doors unresolved |
| `expm1(1e-16)` | `1e-16` | facade `0.0`; Spark SQL door already `1e-16` (datafusion-spark `exp_m1`); ANSI unresolved |
| `expm1(1e-10)` | `1.00000000005e-10` | facade `1.000000082740371e-10` |
| `expm1(-1e-10)` | `-9.999999999500001e-11` | facade `-1.000000082740371e-10` |
| `expm1(1e-5)` | `1.0000050000166668e-05` | facade `1.0000050000069649e-05` |
| `expm1(0)` / `expm1(1)` / `expm1(-1)` / `expm1(-2)` | `0.0` / `1.718281828459045` / `-0.6321205588285577` / `-0.8646647167633873` | facade same |
| `expm1(700)` / `expm1(710)` | `1.0142320547350045e+304` / `inf` | facade same |
| `expm1(NULL)` / `expm1(NaN)` | NULL / nan | Spark SQL door same; ANSI unresolved |
| `expm1(INT/DECIMAL 1)` / `expm1(DECIMAL 1e-16)` | `1.718…` / `1e-16` | Spark SQL door same; ANSI unresolved |
| `log(8)` Spark door / ANSI door | `2.0794415416798357` / (ANSI `0.9030899869919434`) | unchanged SEM-1 |
| `ln(8)` / `log2(8)` | `2.0794415416798357` / `3.0` | unchanged |

Charter parenthetical `log1p(-1) → -Infinity`, `log1p(-2) → NaN` is **not** live Spark SQL.
Spark NULLs `x <= -1` (including `-Infinity`) under ANSI on and off. Pins use NULL.

DECIMAL coerce: Spark SQL and (after the kernel) both repark doors return `1e-16` for
`CAST('0.0000000000000001' AS DECIMAL(38,16))`. No door split. No HALT.

## Kernels

| Name | Kernel | Registration |
|---|---|---|
| `log1p` | Arrow `unary(ln_1p)` then `nullif` on `lt_eq(x, -1.0)` | `register_all`; `AnsiDialect.on_session_built`; native `PyReparkSession::native` |
| `expm1` | Arrow `unary(exp_m1)` | same; overwrites datafusion-spark `SparkExpm1` on the Spark door |

Arrow `unary` vs Option-collect (reviewer measure, 2026-09-02): expm1 14.5–14.9 → 12.8–13.1 ns/row dense, 16.7–17.3 → 12.2 at 10 % nulls; log1p 14.5–16.2 → 12.7–12.9 ns/row. Domain cells still match after the swap.

`docs/design/v1-0-api-review-2026-09-02.md` row J1 and the freeze register still list `expm1` as excepted because `BL-15` was open at their base. No edit to the packet; `python3 scripts/build_api_freeze.py --write` is a later intended change.
| facade | `_scalar("log1p")` / `_scalar("expm1")` | `function_dispatch.rs` + `expr_fn.rs` |

## Mutation

Restore composed `log(1+col)` / `exp(col)-1` in `functions_expr.py` (2026-09-02):
**4 red of 35** collected (`test_log1p_1.py` + BL-15 pin; 30 passed, 1 skipped live).
Reds: `test_facade_tiny_argument_is_not_the_composed_form`,
`test_facade_matches_sql_on_the_measured_grid`,
`test_facade_source_is_the_kernel_not_composition`,
`test_bl15_expm1_matches_spark_precise_kernel`. SQL-door cells stayed green.
Restored the kernel wrappers.

## Sequence

1. Pickup `make ledger-archive` (3 completed ledgers).
2. Measure live Spark. Record table.
3. Kernels + facade + three-door pins + example + BL-15 FIXED.
4. Gates. Ledger `move` to `completed/`.
5. Remediation (2026-09-02): live cell moved onto `test_parity_live.py` `spark_engine`
   (one SELECT r0–r8, no Ivy, no `stop`); `spark_log1p` register only in `native()`;
   restored session.rs pool-opt-out comment; Arrow `unary` kernels.

Co-collect (`REPARK_PARITY_LIVE=1`, 2026-09-02):
| Order | Result |
|---|---|
| `test_live_disclosure_still_diverges` then `test_log1p_1.py` | 46 passed |
| `test_log1p_1.py` then `test_live_disclosure_still_diverges` | 46 passed |
| disclosure then `test_live_log1p_expm1_tiny_args_and_domain` then `test_live_scenario_matches_repark_golden_and_spark` | 56 passed |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: log1p-1-precise-kernels
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Three-door pins vs live PySpark 4.1.2; tiny-arg cells bit-equal to ln_1p/exp_m1; domain NULL at x<=-1.
      artifacts: [python/repark/tests/test_log1p_1.py, crates/repark-functions/src/spark_log1p.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Grid covers 1e-16 through 710, NULL, NaN, INT, DECIMAL; live cell re-derives the tiny-arg and domain rows.
      artifacts: [python/repark/tests/test_log1p_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Invalid function is gone; domain NULL is the Spark answer not an error.
      artifacts: [python/repark/tests/test_log1p_1.py]
    - id: AT-4
      status: N/A
      justification: Scalar UDFs are immutable and have no shared mutable state.
    - id: AT-5
      status: ATTACKED
      evidence: No AWS, IAM, secrets, or .github change. No dependency-file change.
      artifacts: [crates/repark-functions/src/spark_log1p.rs]
    - id: AT-6
      status: ATTACKED
      evidence: Null slots with live buffer values still NULL out; NULL in NULL out on both kernels.
      artifacts: [crates/repark-functions/src/spark_log1p.rs]
    - id: AT-7
      status: N/A
      justification: Unary f64 kernels; no new allocation shape beyond the existing log UDF.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml or lockfile change. Uses f64::ln_1p / f64::exp_m1.
      artifacts: [crates/repark-functions/src/spark_log1p.rs]
    - id: AT-9
      status: ATTACKED
      evidence: BL-15 FIXED; PY_COMPOSED drops log1p/expm1; no new log1p registry row.
      artifacts: [docs/spark-sql-iceberg-parity.md, docs/design/spark-function-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Five clauses pinned; maps lockstep; facade composition mutation 4 red of 35 then restored.
      artifacts: [python/repark/tests/test_log1p_1.py, crates/repark-functions/src/map.md]
```
