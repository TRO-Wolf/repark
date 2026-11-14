# ROUND-DOUBLE-EXACT-1 step 0 — the inventory

**Revised 2026-11-14 after the inventory critic** (Muse 1.3, xhigh, `VERDICT: GAPS`, seat judged
correct): rows A8, A9, B5 and C6 to C10 are added for the critic's M1 to M5 and M7, the citations W1 to
W3 are corrected, and the native gap is recorded (orchestrator ruling 2). Each change is marked.

By the insertion-inventory rule (owner, 2026-11-09). The property: **a FLOAT or DOUBLE value is rounded by
`round` or `bround`**, wherever the call is resolved, planned, folded or reached. Each seat has its
file:line, the kernel it reaches, and "seen: yes / no, because". Dependency lines are the cargo registry
copies the lock pins: `datafusion-functions-54.1.0`, `datafusion-spark-54.1.0` and
`datafusion-optimizer-54.1.0`. RePark lines are at `954423e0`, main when this step was cut.

## How the inventory was searched (repeatable)

1. **Every name match in RePark:** `grep -rn --include=*.rs -E '"round"|"bround"' crates`, with `tests/`
   and `*_tests.rs` set aside. It hits four files: `function_dispatch.rs`, `dispatch_spark.rs`,
   `cardinality.rs` and `spark_math/bround.rs` (its own `"bround"` literals). *Corrected (W2): the first
   version said three.*
2. **Every kernel or builder:** `grep -rn --include=*.rs -i -E 'bround|RoundFunc|SparkRound|
   expr_fn::round|round_udf' crates`, less tests.
3. **The facade:** `grep -rn -E '\bb?round\b' python/repark/src/repark --include=*.py`.
4. **Every function registry a door builds:** `grep -rn --include=*.rs -E 'register_all\b|
   all_default_scalar_functions|register_native_door_functions|fn register\(&self, ctx' crates`, less tests.
   It finds the Spark-door session (`SparkExtension::register`), the `F.expr` context
   (`build_expr_context`) and the native session (`PyReparkSession.native`).
5. **DataFusion:** `grep -rn '"round"'` over `datafusion-functions-54.1.0/src` and
   `datafusion-spark-54.1.0/src`; `grep -n 'make_udf_function!(round'` in each `math/mod.rs`; `grep -n
   'struct ConstEvaluator\|fn volatility_ok'` in `simplify_expressions/expr_simplifier.rs`.
6. **Measured confirmation of which kernel a door reaches**, from [raw/](raw/map.md) and
   [domain-table.md](domain-table.md):
   - an INT argument stays INT on the SQL door (datafusion-spark) and becomes DOUBLE on the native door
     (DataFusion core);
   - a column scale fails with datafusion-spark's own text, `Unsupported type for round scale: Int32`, on
     the SQL and `F.expr` doors, and answers per row on `F.round` and the native door;
   - the DOUBLE `round` SELECT cells give 292 non-EQUAL on SQL and `F.expr` against 301 on `F.round` and
     native: two kernels, two counts;
   - the native door answers `UNRESOLVED_ROUTINE` for `bround`.

## A. Resolution: which `round` each door gets

| # | door | seat | kernel | seen? |
|---|---|---|---|---|
| A1 | Spark-door SQL `round` | `repark-spark/src/extension.rs:168-169` (`SparkExtension::register` calls `register_all`); `repark-functions/src/lib.rs:104-107` registers `datafusion_spark::all_default_scalar_functions()`, whose `round` (`datafusion-spark-54.1.0/src/function/math/mod.rs:46`, `:106`) replaces DataFusion's by name | datafusion-spark `SparkRound` (B2) | **yes** (measured, item 6) |
| A2 | Spark-door SQL `bround` | `lib.rs:160-174`, the later registration loop, chains `spark_math::functions()` (`spark_math.rs:52`, `bround_udf()` at `:59`) after the datafusion-spark set of `:104-107`. *Corrected (W3): was `:163`* | RePark `SparkBround` (B3) | **yes** |
| A3 | `F.round` | `python/repark/src/repark/spark/functions_expr.py:308-310` → `_scalar("round", …)` → `PyColumnParts.call_scalar` → `crates/repark-python/src/column/function_dispatch.rs:250-257`, `datafusion::functions::expr_fn::round` (`use` at `:48`) | DataFusion core `RoundFunc` (B1) | **yes** (measured; `column/door_parity_tests.rs:30-34` lists the split as an expected divergence) |
| A4 | `Column.round` | `python/repark/src/repark/spark/column.py:723-734` → `functions.round` (A3) | B1 | yes, through A3 |
| A5 | `F.bround` | `functions_math.py:12-14` → `function_dispatch.rs:357` → `function_dispatch/dispatch_spark.rs:76`, `:106-115` (`call_bround`) → `spark_math/bround.rs:65` | B3 | **yes** |
| A6 | `F.expr` | `crates/repark-python/src/column/expr_build.rs:276-283` (`sql_context`) → `build_expr_context` `:288-321`: `.with_default_features()` `:309`, then `register_all` `:316`. *Corrected (W1): was `:310`* | B2 for `round`, B3 for `bround` | **yes** (its counts equal the SQL door's in every context) |
| A7 | native door `repark.sql` | `crates/repark-python/src/session.rs:150-165` (`PyReparkSession.native`): DataFusion's default function set plus `session_runtime.rs:436-440` (`log1p`, `cast_map` and a timestamp cast); no Spark registry | `round`: DataFusion core (B1; `datafusion-functions-54.1.0/src/math/mod.rs:187`, `:313`). **No `bround`** | **yes** (`UNRESOLVED_ROUTINE` for `bround`; INT becomes DOUBLE) |

| A8 | `selectExpr` *(added, M1)* | `python/repark/src/repark/spark/filter_quote.py:580-582` (`_select_expr_frame`): runs `session.sql` over a scratch temp view | A1 | **yes**, the same seat (critic's probe: the SQL door's answers) |
| A9 | `filter(str)` *(added, M1)* | `python/repark/src/repark/spark/dataframe/core.py:1117` (`DataFrame.filter`): plans the string through `filter_sql` on the Spark-door frame | A1 | **yes**, the same seat |

The native door does share the name `round`, and it gets DataFusion's own function, not the Spark one.

## B. The kernels: where a float's digits are decided

| # | kernel | file:line | how it rounds | seen? |
|---|---|---|---|---|
| B1 | DataFusion core `round` | `datafusion-functions-54.1.0/src/math/round.rs:633-642` (`round_float`): `(value * 10^dp).round() / 10^dp` in the input's own float type; return field `:225-274` (FLOAT stays FLOAT; any other non-decimal becomes DOUBLE, integers too); invoke `:280` | binary multiply, half away from zero, binary divide. `10^dp` is infinite past 308, so the answer is NaN; a negative `dp` multiplies by an inexact `10^dp` | **yes:** `F.round`, `Column.round`, native |
| B2 | datafusion-spark `round` | `datafusion-spark-54.1.0/src/function/math/round.rs:189-205` (`round_float`): the same multiply, `.round()` and divide, and a divide branch for a negative scale; `return_type` `:118-120` (the input type, so a DECIMAL keeps its scale); `get_scale` `:130-165` (a scalar scale only; a column scale is an execution error); ANSI read from `execution.enable_ansi_mode` `:123`, which RePark never sets | binary | **yes:** Spark-door SQL, `F.expr` |
| B3 | RePark `bround` | `crates/repark-functions/src/spark_math/bround.rs:174-192` (`round_float`): `(value * 10^s).round_ties_even() / 10^s`; a FLOAT is widened and answers DOUBLE (`:143-146`, `:371-374`) | binary, ties to even | **yes:** every door except native |
| B5 | extreme scales *(added, M7)* | B2 answers the input when `10^s` is infinite and `+0.0` for a huge negative scale; B1 computes `inf / inf` and answers **NaN** from scale 309 | B2 matches Spark at the extremes; B1 does not | **yes:** the native door's `ROUND(v, 309)` is NaN, a recorded native gap |
| B4 | K1, step 1's kernel | `crates/repark-common/src/java_double/mod.rs:670-744` | JDK 17 text, half up | **not reached** by `round` today. It is the proposed seat ([spark-rule.md](spark-rule.md)) |

## C. Planning, folding and other reaches

| # | seat | file:line | seen? |
|---|---|---|---|
| C1 | constant folding of `ROUND(literal, s)` | `datafusion-optimizer-54.1.0/src/simplify_expressions/expr_simplifier.rs:482` (`ConstEvaluator`), `:614-616` (an Immutable function folds); the fold calls the same `invoke_with_args` | **yes:** the `lit` context answers exactly as the `sel` context on every door. The fold runs the door's runtime kernel |
| C2 | signature coercion | B1 `RoundFunc::new` (`round.rs:181`), B2 `SparkRound::new` (`round.rs:58`): `float64` is `Coercion::new_implicit` from `Numeric`, and the scale is Int32 implicit from any integer | **yes:** a BIGINT scale literal is accepted on both. A string argument is refused on both, where Spark with ANSI on answers 1.01 (`card_string_arg`, a side finding outside this card) |
| C3 | the plan-time cardinality ceiling | `crates/repark-functions/src/cardinality.rs:323-333`, `:405` | **no**, because it folds `round(x)` and `round(x, 0)` in f64 only to bound the size of an array-expanding function (`sequence`, `array_repeat`); it never yields a value a query returns |
| C4 | any rule that names `round` | step 1 of the search finds nothing beyond A3 and C3; no RePark analyzer rule rewrites a `round` call | **no** |
| C6 | temp views *(added, M2)* | a view stores its resolved plan, so it reuses the UDF bound when it was created | **yes**, the same seat: a view created after the fix gets the new kernel |
| C7 | `PREPARE` *(added, M2)* | RePark has no `PREPARE` statement (`session.rs` only prepares the query text, `:98`) | **no**, because there is no such door |
| C8 | nested queries and subqueries *(added, M5)* | `SELECT ROUND(x, 2) FROM (SELECT …)`, `ROUND(ROUND(v, 3), 2)`, a `WHERE ROUND(v, 2) = …` subquery | **yes**, the same seat (the critic's probe matches the door, cell for cell) |
| C9 | `format_number` *(added, M3)* | `crates/repark-functions/src/string/format_number.rs:135-217`: its own half-even routine over decimal strings | **no**, because it never reaches B1, B2 or B3 (`FORMAT_NUMBER(12345.6789, 2)` is `12,345.68` on both engines) |
| C10 | `ceil`, `floor`, `try_round` *(added, M4)* | `function_dispatch.rs:125-132`: one argument only, DataFusion core kernels; no `try_round` exists in `crates` | **no**, because there is no form with a scale |
| C5 | Python display | `column_render.py:62-64`; `functions.py:1169`, which carries a window spec through `round` | **no**, because these are names and window specs only |

## D. Where the result is stored

| # | target | seat | seen? |
|---|---|---|---|
| D1 | a DOUBLE or FLOAT column | a plain write | **yes:** each store cell equals its SELECT cell (the `store` context in [domain-table.md](domain-table.md)) |
| D2 | a DECIMAL column | step 1's K1 at the conform stage and the batch store (`DOUBLE-DECIMAL-STORE-ROUND-1`) | **yes:** a wrong `ROUND` value then stores a wrong decimal, as with `ROUND(1.005, 2)` into `DECIMAL(10,2)`, which stores `1.00` against Spark's `1.01`. Where `ROUND` is right, K1 stores Spark's decimal |

## The seat this points to

Spark's rule is one kernel for both functions and both float types ([spark-rule.md](spark-rule.md)). RePark
reaches three binary kernels (B1, B2 and B3) through four doors. Below are the fewest seats that cover every
door where `round` or `bround` reaches a FLOAT or DOUBLE. The hand-back to the orchestrator carries the
file count and the open questions.

1. **The kernel**, beside K1 in `java_double/mod.rs`: `round` and `bround` for `f64` and `f32` with an
   `i32` scale, built from `dtoa_double_bits`, the digit rounding and the product fast path, with the
   four edges E1 to E4.
2. **One RePark `round` UDF** for the FLOAT and DOUBLE arms. Every other type delegates to the kernel the
   door has today, so no INT or DECIMAL control moves. It is registered after datafusion-spark's in
   `register_all`, which covers A1 and A6, and `function_dispatch.rs:250-257` builds it, which covers A3
   and A4.
3. **`bround`'s float branch** (B3) calls the same kernel with half even and keeps FLOAT a FLOAT.
4. **The native door** (A7) stays as it is (orchestrator ruling 2, after the owner's Q2 and native-gap
   rulings). **The recorded native gap:** 11,158 `round` cells that differ from Spark per mode, `bround`
   missing (71,763 cells), and `ROUND(v, 309)` answering NaN (B5).

**Literal spellings (critic M8).** A bare `1.005` in Spark-door SQL is a DECIMAL literal, so it reaches the
DECIMAL arm. Only a `CAST('…' AS DOUBLE)` or `CAST('…' AS FLOAT)` spelling reaches the float arms, and every
pin uses those spellings.

Up: [map.md](map.md).
