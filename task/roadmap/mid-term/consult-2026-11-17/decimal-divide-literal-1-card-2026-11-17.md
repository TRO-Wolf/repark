# Card DECIMAL-DIVIDE-LITERAL-1: a DECIMAL divided by an integer literal takes the wrong type, and the wrong digits when the quotient does not terminate

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 2 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 1, with the probe outputs Fable named (`p2-spark`, `p2-pkg-cu-88999454` and `p2-pkg-dct-7bf32859`; kept outside the repository).

**Status:** built, not merged (2026-11-18; delta 2 built, below).

**Severity: S1, label-blocking** (ruled 2026-11-17, ruling 2).

## Finding

Let `d` be a `DECIMAL(10,2)` column. In a division, RePark types an integer literal as `decimal(10,0)`. Spark types it by its digits, `decimal(1,0)` for `3`. So the result type differs on every such division, and the digits differ whenever the quotient does not terminate.

Fable's reading (a reading, not checked against Spark's source): multiplication and addition already follow Spark, and only the division differs. The registry row DEC-5 in [the parity registry](../../../../docs/spark-sql-iceberg-parity.md) states the literal rule for multiplication: "the integer literal `5` is `DECIMAL(1,0)`".

Fable's reason for severity (section 1): "by the rulings' classes it is a silent difference on the Spark door under ANSI on." The card is S1 by ruling 2.

## Cells

Measured on main and on #1350's head, ANSI on, Spark 4.1.2. `d` is `DECIMAL(10,2)` (Fable's §1 table; the probe outputs agree):

| cell | Spark | main `pkg-cu-88999454` | #1350's head `pkg-dct-7bf32859` |
|---|---|---|---|
| `d / 3` (d = 0.30) | `decimal(14,6)`: 0.100000 | `decimal(21,13)`: 0.1000000000000 | the same |
| `d / 100` | `decimal(14,6)`: 0.003000 | `decimal(21,13)`: 0.0030000000000 | the same |
| `1.0 / 3` | `decimal(7,6)`: 0.333333 | `decimal(13,12)`: 0.333333333333 | the same |
| `d * 2`, `d + 1`, `d * 1.1` | `decimal(12,2)`, `decimal(11,2)`, `decimal(13,3)` | the same | the same |
| `d / b`, `b` an `INT` column | `decimal(21,13)` | the same | the same |

Multiplication and addition agree with Spark. Division does not.

## Correction to the INTDIV-1 ledger

Residue R-INTDIV-2 of [intdiv-1-ledger.md](../../../ledgers/archive/2026-10/2026-10-30-intdiv-1-ledger.md), dated 2026-09-28, says "values equal". Fable's `1.0 / 3` cell (0.333333 against 0.333333333333) shows the values differ when the quotient does not terminate. This card records that correction. The ledger is not edited.

## Seat

Not diagnosed. The orchestrator's lead is "the division function from #99 that owns `/0`". The registry row DEC-7 names "the U4b division UDF that owns `/0` (#99)". This is a reading only.

## Neighbour

Fable: "It sits beside AVG-DECIMAL-ROUND-1, since both are Spark's decimal division." AVG-DECIMAL-ROUND-1's step 0 carries these cells as a neighbour. If they share a seat, they fold in; if not, this card stays its own unit (ruling 1).

Up: [map.md](map.md).

## Append (2026-11-17, afternoon): scope after rulings 1, 2, 3 and 10, the seats, and the place

- **Scope** (rulings 1 to 3 and 10, the [afternoon ruling](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)): seat A2. It takes the literal width of `/` and `%`; the DataFrame `/` fault, where any decimal operand answers `double`; the suffixed-literal `*` (`3S`, `3L`); and the `mod()` and `pmod()` calls, with a function-call arm. It does **not** take `round()`, which stays DECIMAL-ROUND-RESULT-TYPE-1. Its pin is `avg(d / 3)`: after AVG-DECIMAL-ROUND-1 alone that cell stays wrong (`shape/avg_div/*`).
- **Cells** (sketch §6): Q2, `divdf/col_div_int/*`, `col_div_lit`, `col_div_intcol`, `int_rdiv_col` and `where_round` (50 DataFrame cells, ANSI off), and the revision's `moddf/round_col_div/*`; Q3, `extra/mul_smallint_literal`, `extra/mul_bigint_literal`, `divctl/mod/*/i3`, `rev: mod/mod/*`, `mod/pmod/*`, `moddf/F.pmod/*` and `moddf/pct/*` (type only; values equal); Q10, `shape/avg_div/*`.
- **Seats the step 0 found** (inventory, "Division"; sketch §4, A2). These replace the undiagnosed reading of the seat above:
  - `SparkDecimalPrecision`, U3 literal step, at `crates/repark-functions/src/decimal_precision.rs:65-70`. It covers `+`, `-` and `*` only (`is_decimal_arithmetic`), so `/` and `%` never reach it.
  - `PyColumn::div` at `crates/repark-python/src/column/mod.rs:697-703`. It casts both operands to `Float64` before `/`, which is the DataFrame fault.
  - The `mod` and `pmod` calls: datafusion-spark's `SparkMod` and `SparkPmod`, `datafusion-spark-54.1.0/src/function/math/modulus.rs:103-163`. U3 walks `BinaryExpr` only, so a function-call arm is needed.
- **Place** (ruling 1): next after AVG-DECIMAL-ROUND-1, which is order 1. This card is order 2 (sketch §5, "Order: 1 then 2").

Up: [map.md](map.md).

## Built (2026-11-18): seat A2, branch `fix/decimal-divide-literal-1`

**Status: built, not merged.** Builder: Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. Product commit `a30a8d60`;
package `pkg-ddl-a30a8d60` (its `BUILT_FROM`). Evidence: [decimal-divide-literal-1/](../../../wo/decimal-divide-literal-1/map.md).

- **Rulings carried (owner, verbatim):** "Defaults" (2026-11-17, ruling 2: S1, label-blocking; the label is scoped to
  ANSI on); "Merge 1352 and 1353, defaults on the questions" (2026-11-17: Q1 two units, the `mod()`/`pmod()` arm rides
  this seat; Q2 this unit takes the DataFrame `/`; Q3 the suffixed `*`, `%`, `mod()`/`pmod()`, not `round()`; Q5
  `allowPrecisionLoss=false` carded; Q8 `d38_10 * 3` carded; Q9 the native door carded and unchanged; Q10 the
  `avg(d / 3)` pin is this unit's); "dispatch Opus xhigh effort levels for these fixes, then use Opus 5.5 medium
  verifiers for the time and not muse, at least until these things are closed out" (2026-11-17); "Proceed with the
  plans, use defaults, when it comes to any roadblocks or questions spin up a Fable medium session to ask for
  guidance. Merge any PRs needed as long as they get us to our end goal" (2026-11-18).
- **Seat and files (5 product files, at the cap):**
  - `crates/repark-functions/src/decimal_precision/literal_step.rs` (new): the U3 functions (moved), the bottom-up `/`
    and `%` literal step, the `mod()`/`pmod()` call arm, the DataFrame divide marker, `spell_integer_suffix`, the
    `pickMinimumPrecision` conf;
  - `crates/repark-functions/src/decimal_precision.rs`: the rule runs the bottom-up pass before the `+ − *` pass,
    reads the conf, and the suffix arm joins U3 (`min_precision_integer_literal`); `Int8`/`UInt8` are not narrowed;
    still installed before `type_coercion` (`analyzer_rules_with_higher_order_preparation`) and after it;
  - `crates/repark-spark/src/spark_rewrites/mod.rs`: `3S` and `3L` beside an arithmetic operator or in a `mod`/`pmod`
    call are spelled with the suffix marker;
  - `crates/repark-python/src/column/display.rs` (the facade's `_binary` `/`) and `column/mod.rs` (`PyColumn::div`):
    the DataFrame divide marker in place of the two `Float64` casts;
  - one statement in `crates/repark-spark/src/extension.rs` installs the conf at session build.
- **The bar** (step 0's probes, 7,258 comparisons against Spark 4.1.2, both ANSI modes; [bar.md](../../../wo/decimal-divide-literal-1/evidence/built/bar.md)):
  0 EQUAL → WRONG, 0 EQUAL → RAISE; the native door, `avg`/`mean`/`sum`, `try_*`, typed operands and `d / (1 + 2)`
  unchanged; 1,088 cells became EQUAL (every literal width of `/` and `%`, the DataFrame `/` and `%`, `mod`/`pmod`,
  the suffixed `*`, four `h / 2` scopes, `avg(d / 3)`, `WHERE`/`CASE`, every division wrong write). Left in scope,
  by class: the 38-digit row 10 both raising (ANSI on); `round()`'s type (DECIMAL-ROUND-RESULT-TYPE-1); the
  session-catalog view's outer `/` (`decimal(18,10)` against `decimal(16,8)`, values equal; no card yet); the
  38-digit multiplication overflow (the `d38_10 * 3` card); `try_*` (TRY-ARITH-DECIMAL-INT-1);
  `allowPrecisionLoss=false` (DECIMAL-PRECISION-LOSS-CONF-1); the native door (NATIVE-DECIMAL-AVG-DIVIDE-1).
- **Pins:** `python/repark/tests/types/test_decimal_divide_literal_1.py` — 162 passed on the head, 118 failed and 44
  passed (the guards) on main; Rust `decimal_precision/literal_step_tests.rs`. Re-pinned to Spark's answers:
  `dataframe/test_columns.py::test_division_is_float` (`decimal(8,6)`; an integer pair stays `double`) and the four
  `dec-div0` cells of `writes/cast_overflow_insert_1_spark_oracle.json` (RePark's message now names Spark's
  `DECIMAL(8,6)`).
- **Cost** ([cost.md](../../../wo/decimal-divide-literal-1/evidence/built/cost.md); two alternating runs, stopped short of
  five and ten by a shared measure lock and the turn budget): the SQL division
  rows are faster on the head (Spark's narrower quotient), the integer and float controls on both doors and C-012 are
  at the bar; **two held rows are over 1.05x and outside their control bands — a halt, not ruled here:** the
  DataFrame `col(d10) / 3` (about 3.7x: main's `double` division against the decimal division UDF Spark's type
  needs; the same cost as the SQL `d10 / 3` on the head) and `F.expr("mod(d10, 3)")` (about 1.2x: the per-row cast
  to Spark's `Remainder` type; a remedy inside the cap, not built: the `%` path).
- **Gates** (exit codes): `cargo test --locked` `repark-functions` 0, `repark-common` 0, `repark-core` 0,
  `repark-spark` 0 (on `a30a8d60`; 101 on the first commit, the reverted nullability line), `repark-python` 0;
  `make rust-clippy` 0; `make rust-panic-ban` 0; `cargo fmt --all -- --check` 0 (`make rust-fmt-check` is the
  same command); the facade suite, release build, `-n 6`: 35,321 passed and 9 failed on the first commit (four
  from the reverted line, five re-pinned to Spark), and the affected files with both units' pins 809 passed on
  `a30a8d60`; `make py-test-facade`: not run (the cost halt); `pytest -n 6 python/repark-parity/tests` 0 with the
  records; `ledger_lifecycle.py check` 0; `check_map_size.py` 0 (three exact baselines lowered); `sync_map_md.py
  --check` 0; the comment gate 0 hits.
- **Found beside (not this unit's):** a written `CAST(3 AS DECIMAL(20,0))` in `*` is still narrowed by U3's
  default-cast check (`d10_2 * CAST(3 AS DECIMAL(20,0))` is `decimal(12,2)`, Spark `decimal(31,2)`; main alike);
  a runtime `SET` of `pickMinimumPrecision` is accepted and not read (the builder conf is).

### Built, delta 1 (2026-11-18): a decimal `mod()` on the `%` path

**Status: built, not merged; delta 1 halted (below).** Builder: Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh.
Product commit `8efe8e69` (over `a30a8d60`); package `pkg-ddl-8efe8e69` (its `BUILT_FROM`). Evidence:
[bar.md](../../../wo/decimal-divide-literal-1/evidence/built/bar.md) §0 and
[cost.md](../../../wo/decimal-divide-literal-1/evidence/built/cost.md).

- **Guidance carried:** Fable's reply on the build's cost halt (`claude-fable-5-2`, 2026-11-18), **guidance under the
  owner's overnight delegation, not an owner ruling**: Q1, the DataFrame `col(d10) / 3` row is the owner's (report
  three ratios, no verdict); Q2, measure `mod(d, 0)`, `d % 0`, `pmod(d, 0)` on Spark first and put a decimal `mod()` on
  the `%` path if Spark raises; time SQL `pmod()` and rewrite it only onto `F.pmod`'s spelling, measured against
  Spark; Q3, the full series on the delta's package; Q4, card the items the build left.
- **Spark first:** with ANSI on, `mod(d, 0)`, `d % 0`, `pmod(d, 0)` and `mod(d, CAST(0 AS DECIMAL(5,2)))` raise
  `[REMAINDER_BY_ZERO]` at `d10_2`, `d28_2` and `d38_10`, on SQL and the DataFrame door; with ANSI off they answer
  NULL. `mod` and `%` take the dividend's sign; `pmod(d, -3)` answers `1.25` for `7.25`.
- **Built (one product file, the cap stays at 5 plus one statement):** `literal_step.rs` `remainder_node` rewrites a
  decimal `mod()` onto the `%` operator (`remainder_operator`, `remainder_decimal_operand`; an integer argument cast to
  its `forType` width). Every non-zero `mod()` cell keeps its answer and type; the class **"NULL → RAISE, Spark's
  answer"** is 22 cells (ANSI on, `mod(d, 0)` by a literal, a decimal cast or a zero column; SQL, `F.expr`,
  `selectExpr`; the pin file's five), each raising `[DIVIDE_BY_ZERO]` for Spark's `[REMAINDER_BY_ZERO]`. `pmod()` is
  not rewritten: `F.pmod`'s spelling answers a negative divisor unlike Spark.
- **The bar:** step 0's whole bar re-ran on the delta's head and every raw cell answers as on the build's head (0 EQUAL
  → WRONG, 0 EQUAL → RAISE, the protected cells unmoved). Pins: 182 passed on the head; 137 failed and 45 passed on
  main (the new green on main: `d10_2 % 0` with ANSI on).
- **Cost** (five runs per row, ten over the bar): `F.expr("mod(d10, 3)")` 0.794x (was 1.207x); SQL `mod(d10, 3)`
  0.869x; `F.pmod` 0.990x after ten; the build's near rows under. **SQL `pmod(d10, 3)` 1.142x after ten, outside its
  band (0.950 to 1.064): a halt.** The DataFrame `col(d10) / 3`, the owner's row: 3.935x against main's `double`
  division, 0.767x against main's SQL `d10 / 3`, 0.754x against main's `F.expr("d10 / 3")`.
- **Found by the delta, a halt:** after a runtime `SET spark.sql.legacy.literal.pickMinimumPrecision=false` (accepted,
  read back, not routed to the analyzer), `d10_2 / 3` and `d10_2 % 3` keep the narrowed widths (`decimal(14,6)`,
  `decimal(3,2)`) where Spark and main answer `decimal(21,13)` and `decimal(10,2)`: EQUAL on main, TYPE on the head,
  both modes. The fix routes the key at runtime (`builder_conf.py`, `session_runtime.rs`), outside the cap.
- **Step 5, `hdiv/sessview`:** a CTAS of `h / 2` from a session-catalog view of `d10_2 / 2` over an Iceberg table
  stores `decimal(18,10)` on the head, `decimal(25,17)` on main, `decimal(16, 8)` on Spark; values equal.
- **Proposed cards** (bar.md §0.6; the clerk files them; every severity is a proposal for the owner):
  SESSION-VIEW-DECIMAL-DIV-1 (S2; its label effect is the owner's); a written `CAST(3 AS DECIMAL(20,0))` narrowed in
  `*` and `+` (S3); the runtime `SET` of `pickMinimumPrecision` (S2); the remainder-by-zero error class (S3); ANSI-on
  `pmod(d, 0)` and integer `mod`/`pmod` by zero answering NULL (S2); `F.pmod` with a negative divisor (S2); `pmod()`
  past its `Pmod` type raising Arrow's error (S3).
- **Gates** (exit codes): `cargo test --locked` `repark-common`, `repark-functions`, `repark-core`,
  `repark-spark`, `repark-python` 0; `make rust-clippy` 0; `make rust-panic-ban` 0; `cargo fmt --all -- --check` 0;
  `make rust-fmt-check` 0; the pin file 182 passed on the head; the facade suite, release build, `-n 6`: 35,350
  passed, 0 failed; `make py-test-facade` (`-n 6`): 35,345 passed, 0 failed; `pytest -n 6 python/repark-parity/tests`
  0 with the records; `ledger_lifecycle.py check` 0; `check_map_size.py` 0; `sync_map_md.py --check` 0;
  `uvx ruff@0.15.22 check .` 0 and `format --check .` 0 (the build's scripts restructured, PR #1361's CI failure); the
  comment gate 0 hits.


### Built, delta 2 (2026-11-18): a decimal `pmod()` on Spark's formula, the runtime `SET`, `F.pmod`

**Status: built, not merged.** Builder: Claude Opus 5.5 (`claude-opus-5-5`), effort xhigh. Product commit `5b34022f`
(over `8efe8e69`); package `pkg-ddl-5b34022f` (its `BUILT_FROM`). Evidence:
[bar.md](../../../wo/decimal-divide-literal-1/evidence/built/bar.md) §D2 and
[cost.md](../../../wo/decimal-divide-literal-1/evidence/built/cost.md).

- **Guidance carried:** Fable's reply on delta 1's halts (`claude-fable-5-2`, 2026-11-18), **guidance under the owner's
  delegation, not an owner ruling**: Q1, rewrite a decimal `pmod()` as Spark's formula over `%` (Fable withdrew its
  earlier "only onto `F.pmod`'s spelling"); Q2, route the runtime `SET` of `pickMinimumPrecision` in two exception
  files; Q3, take `F.pmod` as a third exception file with a back-out, the decimal half of card 4b, and leave the
  integer `mod`/`pmod` by zero carded.
- **The cap: 5 product files to 8, on that guidance** (the owner can reverse it): the build's 5 plus
  `python/repark/src/repark/spark/session/builder_conf.py`, `crates/repark-python/src/session_runtime.rs` and
  `python/repark/src/repark/spark/functions_expr.py` (`pmod` only). The PR is at 8.
- **Built:** `literal_step.rs` `pmod_formula` makes a decimal `pmod()` `CASE WHEN r < 0 THEN (r + n) % n ELSE r END`
  with `r = a % n` (computed once per row by the optimizer's common-subexpression pass); no cast where the formula's
  type is `Pmod`'s (the timed row), otherwise `TRY_CAST` with ANSI off and a plain cast with ANSI on; a 38-digit
  divisor keeps `SparkPmod`. `session_runtime.rs` and `builder_conf.py` route `SET`, `unset` and `RESET` of the key,
  refusing a non-boolean as the decimal truncate key does. `F.pmod` spells the same formula (kept: no cell EQUAL on
  delta 1's head moved, and its row is under the bar).
- **The bar:** step 0's bar re-ran on the package and all 7,026 head cells answer as on delta 1's head (0 EQUAL →
  WRONG, 0 EQUAL → RAISE, the protected cells unmoved). The class **"NULL → RAISE, Spark's answer"** gains the 13
  probe and 15 pin cells of an ANSI-on decimal `pmod(d, 0)` (50 in all). The class **"TYPE → EQUAL after the runtime
  `SET`"** is 38 cells (`*`, `+`, `-`, `mod()`, `pmod()`, and `/` and `%` after `SET …=true` over a `false` build).
  Delta 1's halt 2 is closed (`d / 3` and `d % 3` after `SET …=false` are EQUAL again). **One cell moved from EQUAL on
  delta 1's head:** a temp view made before the `SET` and read after it refuses `CANNOT_UP_CAST_DATATYPE` where Spark
  answers the creation setting (main TYPE; TEMP-VIEW-SESSION-SETTINGS-1 does not capture the key; a fix is a ninth
  file, proposed card 7). Pins: 394 passed and 20 strict `xfail`s on the head; 291 failed, 103 passed on main.
- **Cost** (five runs per row, ten for a row over the bar after five): **SQL `pmod(d10, 3)` 0.749x** against main's
  `pmod(d10, 3)` (it was 1.142x on delta 1's head; main answered `decimal(12,2)`), and 1.582x against main's SQL
  `d10 % 3`, the base Fable named, as a measurement only; `F.pmod` 0.843x; every other held row under the bar,
  C-012 1.029x and 1.007x after ten. The DataFrame `col(d10) / 3` is **accepted by the owner** (owner, 2026-11-18, about 13:40 UTC, verbatim: "Go with Fable's leans on A, B and C."; Fable's lean
  B: the named cost of the Q2 S1 fix, measured like for like against main's SQL `d10 / 3`): 3.907x against main's `double`
  division, 0.768x against main's SQL `d10 / 3`, 0.779x against main's `F.expr("d10 / 3")`.
- **Proposed cards, kept current** (bar.md §D2.8): closed by delta 2 — 3 (the runtime `SET`), 4b's decimal half, 4c
  (`F.pmod` by a negative divisor), 4d's ANSI-off half. Open — 1 SESSION-VIEW-DECIMAL-DIV-1 (S2), 2 the written
  `CAST(3 AS DECIMAL(20,0))` (S3), 4a the remainder-by-zero class (S3), 4b's integer half (4 cells; Fable's lean S2),
  4d's ANSI-on class (S3). New — 5 `F.pmod`'s formula type (S3), 6 an integer `F.pmod`'s sum overflowing with ANSI on
  (S3), 7 the temp view and the runtime `SET` (S3). **Severities ruled** (owner, 2026-11-18, about 13:40 UTC, verbatim: "Go with Fable's leans on A, B and C."): SESSION-VIEW-DECIMAL-DIV-1 S2, not
  label-blocking; the written cast S3; the integer `mod`/`pmod` by zero S2; the others as proposed (the runtime `SET`
  card is closed by this delta).
- **Gates:** the delta's hand-back carries the gate table (run on the product commit `5b34022f`).

**Verifier record notes (PR #1361, 2026-11-18).** Copied from the verify verdict's section "Record notes (no measured contradiction)":

- **R2:** card 2 names the written `CAST(3 AS DECIMAL(20,0))` narrowing in `*` and `+`; `-` is narrowed too (`attack1 suf/d10_2/CAST(3 AS DECIMAL(20,0))/-/{l,r}`, main alike).
- **R3:** card 7 names the view created before `SET …=false`; the mirror (built `false`, `SET true`) refuses the same way.

Up: [map.md](map.md).
