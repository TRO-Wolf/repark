# Card ANSI-REMAINDER-CLASS-1: zero remainder raises `DIVIDE_BY_ZERO`, answers NULL, or answers NaN where Spark raises `REMAINDER_BY_ZERO`

**Date:** 2026-10-05 · **Filed by:** Muse Spark 1.3 (contributor), for the orchestrator · **Source:** the orchestrator's Spark 4.1.2 check on PR #943. No product code in this filing.

## Measured (ANSI on, both engines)

Spark 4.1.2 (`/tmp/sparkenv`, banner `spark 4.1.2 ansi=true`) against RePark 1.5.2 (`/tmp/oc-worker/direct/wo/ta-single-series/benchenv`, `spark.sql.ansi.enabled=true`, guard proved live by `SELECT 1 / 0` raising). Probes `/tmp/oc-worker/direct/wo/cards-1005/probe_spark_mod.py` and `/tmp/oc-worker/direct/wo/cards-1005/probe_repark_mod.py`; outputs `/tmp/oc-worker/direct/wo/cards-1005/mod_spark.txt` and `/tmp/oc-worker/direct/wo/cards-1005/mod_152.txt`.

| Cell | Spark 4.1.2 | RePark 1.5.2 |
|---|---|---|
| `SELECT 1.0D % 0.0D` | `REMAINDER_BY_ZERO` | `DIVIDE_BY_ZERO` |
| `SELECT 1.0D % -0.0D` | `REMAINDER_BY_ZERO` | `NaN` (no error) |
| `SELECT pmod(1.0D, 0.0D)` | `REMAINDER_BY_ZERO` | `NULL` (no error) |
| `SELECT mod(5, 0)` | `REMAINDER_BY_ZERO` | `NULL` (no error) |
| `SELECT 5 % 0` | `REMAINDER_BY_ZERO` | `DIVIDE_BY_ZERO` |
| `SELECT try_mod(5, 0)` | `NULL` | `NULL` |
| `SELECT 5 div 0` | `DIVIDE_BY_ZERO` | `ParseException` (no infix `div` parser) |
| `SELECT 5L div 0L` | `DIVIDE_BY_ZERO` | `ParseException` (no infix `div` parser) |
| `SELECT cast(5 as decimal(10,2)) % 0` | `REMAINDER_BY_ZERO` | `DIVIDE_BY_ZERO` |
| DataFrame `(col("id") + 5) % lit(0)` | `REMAINDER_BY_ZERO` | `DIVIDE_BY_ZERO` |

Spark's remainder text, verbatim head: `[REMAINDER_BY_ZERO] Remainder by zero. Use `try_mod` to tolerate divisor being 0 and return NULL instead.` RePark's divide text names `try_divide` instead. Extra cells in `/tmp/oc-worker/direct/wo/cards-1005/probe_repark_mod2.py`: `div(5, 0)` as a function is `UNRESOLVED_ROUTINE`, and `pmod(5, 0)` / `mod(5.0, 0.0)` also answer `NULL`.

## Why

Three separate causes, all read off `main`:

- **One guard, one error class:** the analyzer wraps both `Operator::Divide` and `Operator::Modulo` in the same `__repark_ansi_nonzero_divisor__` UDF (`crates/repark-functions/src/analyzer.rs`, around lines 53–56; the error text in `crates/repark-functions/src/ansi.rs`, around line 320). Every `%` cell therefore raises `DIVIDE_BY_ZERO`.
- **The float zero test misses `-0.0`:** the array path matches `value.to_bits() == 0` (`ansi.rs`, around lines 243–251), so a `-0.0` divisor passes the guard and the kernel answers `NaN`.
- **The function forms bypass the guard:** the analyzer rewrites only the `Divide`/`Modulo` binary operators, never a `ScalarFunction` named `mod` or `pmod`, so both fall through to kernels that answer `NULL`. Infix `div` never reaches any guard: the Spark-door parser has no infix `DIV` arm, and no `div` routine is registered.

## Scope (proposed)

1. **The ANSI guard learns which operator it guards.** The `%`/`Modulo` path raises `[REMAINDER_BY_ZERO] Remainder by zero. Use `try_mod` to tolerate divisor being 0 and return NULL instead.`, keeping the existing `spark.sql.ansi.enabled=false` bypass sentence and the `(ArithmeticException)` trailer; the `/`/`Divide` path keeps today's `DIVIDE_BY_ZERO` text naming `try_divide`.
2. **The float zero test catches `-0.0`.** Match both signed zeros on the float arms (and confirm the scalar path agrees), so `1.0D % -0.0D` raises.
3. **Route `mod`/`pmod` through the same guard** (analyzer rewrite of the function forms, or the guard moved into the kernels — the unit picks after measuring which path the `%` operator and the functions share).
4. **Pins, one row per door per operator:**
   - every matrix cell above answers Spark's class or value on the SQL door, the DataFrame door, and the Rust ANSI door;
   - `try_mod` / `try_divide` still answer `NULL` on zero divisors, and ANSI-off still answers `NULL`/`NaN` instead of raising (the guard must not leak past its conf);
   - `mod`/`pmod` with a nonzero divisor are unchanged (the rewrite must not move values, only the zero-divisor refusal).
5. **Out of scope:** infix `div` parsing plus its `DIVIDE_BY_ZERO` guard — measured here, but a parser unit of its own; it splits into a second card if the unit sizes past small.

## Grade and release

**Lean B, small:** one guard parameterized by operator, one float comparison, one function-form rewrite, all pinned against the matrix above. No sketch needed; the mechanism is measured and the error texts are quoted verbatim.
