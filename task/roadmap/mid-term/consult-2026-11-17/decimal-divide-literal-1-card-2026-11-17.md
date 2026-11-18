# Card DECIMAL-DIVIDE-LITERAL-1: a DECIMAL divided by an integer literal takes the wrong type, and the wrong digits when the quotient does not terminate

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 2 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 1, with the probe outputs Fable named (`p2-spark`, `p2-pkg-cu-88999454` and `p2-pkg-dct-7bf32859`; kept outside the repository).

**Status:** built, not merged (2026-11-18; the build halted at the cost bar, below).

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

Residue R-INTDIV-2 of [intdiv-1-ledger.md](../../../ledgers/completed/intdiv-1-ledger.md), dated 2026-09-28, says "values equal". Fable's `1.0 / 3` cell (0.333333 against 0.333333333333) shows the values differ when the quotient does not terminate. This card records that correction. The ledger is not edited.

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

Up: [map.md](map.md).
