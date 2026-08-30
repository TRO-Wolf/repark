# Charter ledger — F-Y10-1 · integer arithmetic overflow raises where Spark raises

**Date:** 2026-08-30 · **Branch:** `feat/f-y10-1-int-overflow` (opens when the owner confirms
this gate; may run in a fork-wait window) · **Base:** `main` at charter time · **Policy:**
[../../../AGENTS.md](../../../AGENTS.md) "Verify before done" and
[../../../docs/testing.md](../../../docs/testing.md) · **Path:** STANDARD (kernel work in
`crates/repark-functions` on the DEC U5 pattern; the live Spark 4.1.2 oracle is on this box).

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** A wrong answer on ordinary addition outranks any missing function
(`docs/design/spark-function-parity.md` §7.1), and FNP-7b's four `try_*` names are blocked on
this unit: with no raising path they would be no-op wrappers asserting a parity that does not
exist. The item is already tracked (F-Y10-1 / DEC U5 / G13, named in STATUS since the V2
hardening campaign) and the decimal analog DEC-6 is FIXED by exactly the shape this unit
proposes — a checked kernel reading the landed ANSI knob. The tree also carries two
contradictory measurements that must be reconciled before any edit: Y-10 (2026-08-13, Z-4
handoff) observed `CAST(2147483647 AS INT) + CAST(1 AS INT)` **wrap** to Int32 `-2147483648`
on both doors; the FNP-7b row (2026-08-20) measured the same expression **widening** to int64
`2147483648`. At least one description is stale or path-specific; the unit's first clause is
the measurement that settles it.

## PROPOSITION LEDGER — F-Y10-1 — 2026-08-30

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **The behavior matrix is measured before any edit.** For `+`, `-`, `*` on int32 and int64 at the overflow boundary, on all three doors (Spark SQL door, ANSI door, facade expression), with `spark.sql.ansi.enabled` TRUE and FALSE: the current repark result (wrap / widen / raise / NULL) and the live Spark 4.1.2 result are recorded side by side, and the Y-10-vs-FNP-7b wrap-vs-widen contradiction is resolved with the mechanism named (which path widens, which wraps, and why). | The matrix in this ledger, each cell from an executed probe; the contradiction's resolution names the code path. | **PROVEN** | §4 matrix. Both records are live and path-specific. pins: `crates/repark-functions/src/map.md`. |
| C-002 | **Spark door raises where Spark raises.** With `ansi=true` (the landed default), integer `+`, `-`, `*` at the boundary raise `ARITHMETIC_OVERFLOW` exactly where live Spark raises, as shared-raise equality pins on the DEC-6 pattern; with `ansi=false`, the result equals live Spark's non-ANSI result (two's-complement wrap) cell for cell, value and Arrow type. No silent widening remains on any Spark-door path the matrix touched. | Checked kernels in `crates/repark-functions` reading the ANSI knob (DEC U5 shape); red-first corpus pins per cell; oracle read-back. | **PROVEN** | `integer_spark.rs` tests; `test_integer_overflow_parity.py`. C-001 matrix was the red. |
| C-003 | **The ANSI door serves standard SQL.** Overflow on the ANSI door raises per the standard (its own oracle, owner ruling 2026-08-12 Option A) — it does not silently wrap once the Spark-door kernels are checked. Any INTENDED door-vs-door split this creates is pinned in `cross_door.rs` like the six existing ones, not left implicit. | ANSI-door value pins; a cross-door pin per intended split. | **PROVEN** | `ansi_door_int32_add_overflow_raises`; `cross_door_int32_add_overflow_wraps_on_spark_ansi_false_raises_on_ansi`. |
| C-004 | **Documents match the pins.** The registry's routed note (F-Y10-1 under "routed, not invented as DEC rows") moves to a dated FIXED row or an updated finding; gap G13's integer half is closed or narrowed with the residue named; the FNP-7b row in `docs/design/spark-function-parity.md` flips from BLOCKED to unblocked; STATUS; maps in lockstep. | `check-map-sync`, `check-ledger-grammar`, registry diff. | **PROVEN** | Registry F-Y10-1 FIXED 2026-08-30; G13 integer half closed; residue G5b-R3-ANSI and F-Y10-2 named; FNP-7b unblocked. |
| C-005 | **Green on the whole surface, and the hot path is not quietly slower.** `make verify`, `make preflight`, full `make py-test`; the checked kernels' cost on non-overflowing arithmetic is measured (a micro-benchmark or the existing perf harness) and recorded — an order-of-magnitude regression is a finding, not a silent tax. | Gate output; the recorded measurement. | **PROVEN** | `make verify` exit 0; facade 3793 passed / 75 skipped; `make py-test` 459 passed; `make audit` + `workflows-lint` exit 0. Non-overflow Int32 add ratio **1.25** (523 ms / 419 ms, 200 collects). |

VERDICT: PROVEN — 5 clauses, 5 PROVEN, 0 REJECTED. Critic CCC attestation may replace the
Actor-phase coverage block below. The owner confirms.

## 1. Out of scope

- `try_add` / `try_subtract` / `try_multiply` / `try_avg` themselves — FNP-7b builds them after
  this unit closes.
- G5b-R3-ANSI: the ANSI door's negative-interval window RANGE wrap (same G13 campaign body,
  separate unit; pin named in the Z-4 handoff).
- F-Y10-2: ANSI float `/ 0` = IEEE `+Inf` (a recorded residual with an INTENDED cross-door pin).
- Decimal arithmetic — DEC-6 closed it; nothing here reopens `decimal_spark.rs` beyond reading
  the same knob.

## 2. Sequence

1. C-001 matrix first, as a docs-only commit to this ledger — no engine edit before the matrix.
2. C-002 kernels red-first, one operator at a time; C-003 falls out or is pinned as a split.
3. C-005 measurement, C-004 docs, departure.

## 3. Owner actions

- Confirm this gate (the unit is fork-independent and can run in any window).
- No AWS, no IAM, no fork interaction anywhere in this unit.

## 4. C-001 measured matrix (2026-08-30) — no engine edit

**Oracles.** Live Spark 4.1.2 (`pyspark==4.1.2`, `spark.version 4.1.2`, zulu-17,
`master("local[2]")`, `SPARK_LOCAL_IP=127.0.0.1`). Repark from this clone at
`feat/f-y10-1-int-overflow` (charter commit `d3ee7c7`): ANSI door =
`ReparkSession` + `AnsiDialect` (knob not installed); Spark SQL door =
`SparkExtension` + `SparkDialect` with builder
`spark.sql.ansi.enabled` true/false; facade expression =
DataFusion `col("a") + col("b")` / `col("a") + lit(1i64)` on those
sessions (Python `Column.__add__` is that `BinaryExpr`; Python `lit(int)` is
`ScalarValue::Int64`).

**Contradiction.** Neither record is stale. They name different code paths.

| Record | Expression | What it saw | Code path (2026-08-30) |
|---|---|---|---|
| Y-10 2026-08-13 | `CAST(2147483647 AS INT) + CAST(1 AS INT)` | Int32 wrap `-2147483648` on both doors | Same-width `BinaryExpr` → Arrow `arrow-arith` two's-complement kernel. `SparkAnsiConfig` is unread. `SparkDecimalRewrite` matches DECIMAL only. `SparkExprSemantics` rewrites `/` and `%` only. |
| FNP-7b 2026-08-20 | `CAST(2147483647 AS INT) + 1` | Int64 `2147483648` | DataFusion types a bare integer literal as `Int64`. Type coercion promotes `Int32 + Int64` → `Int64`. The sum fits, so there is no wrap. Python `col + 1` is the same `lit(i64)` path. |

The ANSI knob does not change any integer `+ − *` cell today. ANSI door, Spark
`ansi=true`, and Spark `ansi=false` agree cell for cell.

Live Spark 4.1.2 types `1` and `2147483647` as `INT`. `CAST(INT) + 1` stays
`INT`. `ansi=true` raises `[ARITHMETIC_OVERFLOW]` (`integer overflow` or `long
overflow`, with `try_add` / `try_subtract` / `try_multiply` in the remedy).
`ansi=false` wraps and keeps the source type. Spark SQL and Spark facade
`Column` operators agree.

### 4.1 SQL door (value and Arrow type)

`R-A` = repark ANSI. `R-ST` / `R-SF` = repark Spark `ansi=true` / `false`.
`S-T` / `S-F` = live Spark 4.1.2 `ansi=true` / `false`. `wrap` = two's
complement, same width. `widen` = promote to Int64 and return the exact
mathematical sum. `raise` = Spark `ARITHMETIC_OVERFLOW`.

| Cell | SQL | R-A / R-ST / R-SF | S-T | S-F |
|---|---|---|---|---|
| i32 add CAST+CAST | `CAST(2147483647 AS INT) + CAST(1 AS INT)` | wrap Int32 `-2147483648` | raise integer overflow | wrap int `-2147483648` |
| i32 add CAST+lit | `CAST(2147483647 AS INT) + 1` | widen Int64 `2147483648` | raise integer overflow | wrap int `-2147483648` |
| i32 sub CAST+CAST | `CAST(-2147483648 AS INT) - CAST(1 AS INT)` | wrap Int32 `2147483647` | raise integer overflow | wrap int `2147483647` |
| i32 sub CAST+lit | `CAST(-2147483648 AS INT) - 1` | widen Int64 `-2147483649` | raise integer overflow | wrap int `2147483647` |
| i32 mul CAST+CAST | `CAST(2147483647 AS INT) * CAST(2 AS INT)` | wrap Int32 `-2` | raise integer overflow | wrap int `-2` |
| i32 mul CAST+lit | `CAST(2147483647 AS INT) * 2` | widen Int64 `4294967294` | raise integer overflow | wrap int `-2` |
| i32 mul MIN×−1 | `CAST(-2147483648 AS INT) * CAST(-1 AS INT)` | wrap Int32 `-2147483648` | raise integer overflow | wrap int `-2147483648` |
| i64 add CAST+CAST | `CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT)` | wrap Int64 `MIN` | raise long overflow | wrap bigint `MIN` |
| i64 add CAST+lit | `CAST(9223372036854775807 AS BIGINT) + 1` | wrap Int64 `MIN` | raise long overflow | wrap bigint `MIN` |
| i64 sub CAST+CAST | `CAST(-9223372036854775808 AS BIGINT) - CAST(1 AS BIGINT)` | wrap Int64 `MAX` | raise long overflow | wrap bigint `MAX` |
| i64 mul CAST+CAST | `CAST(9223372036854775807 AS BIGINT) * CAST(2 AS BIGINT)` | wrap Int64 `-2` | raise long overflow | wrap bigint `-2` |
| i64 mul MIN×−1 | `CAST(-9223372036854775808 AS BIGINT) * CAST(-1 AS BIGINT)` | wrap Int64 `MIN` | raise long overflow | wrap bigint `MIN` |
| untyped add | `2147483647 + 1` | Int64 `2147483648` (literals infer Int64) | raise integer overflow (literals are INT) | wrap int `-2147483648` |
| i32 add control | `CAST(2147483646 AS INT) + CAST(1 AS INT)` | Int32 `2147483647` | int `2147483647` | int `2147483647` |
| i32 mul control | `CAST(46340 AS INT) * CAST(46340 AS INT)` | Int32 `2147395600` | int `2147395600` | int `2147395600` |

i64 CAST+lit wraps rather than widens because the literal is already Int64, so
the op stays Int64 and Arrow wraps.

### 4.2 Facade expression

| Cell | Expression | R-A / R-ST / R-SF | S-T | S-F |
|---|---|---|---|---|
| i32 add cols | int32 + int32 | wrap Int32 `-2147483648` | raise integer overflow | wrap int `-2147483648` |
| i32 add i64 lit | int32 + lit(1) | widen Int64 `2147483648` | raise integer overflow (Spark `lit(1)` is INT) | wrap int `-2147483648` |
| i32 sub cols | int32 MIN − 1 | wrap Int32 `2147483647` | raise integer overflow | wrap int `2147483647` |
| i32 mul cols | int32 MAX × 2 | wrap Int32 `-2` | raise integer overflow | wrap int `-2` |
| i32 mul MIN×−1 | int32 MIN × −1 | wrap Int32 `-2147483648` | raise integer overflow | wrap int `-2147483648` |
| i64 add cols | int64 MAX + 1 | wrap Int64 `MIN` | raise long overflow | wrap bigint `MIN` |
| i64 sub cols | int64 MIN − 1 | wrap Int64 `MAX` | raise long overflow | wrap bigint `MAX` |
| i64 mul cols | int64 MAX × 2 | wrap Int64 `-2` | raise long overflow | wrap bigint `-2` |
| i64 mul MIN×−1 | int64 MIN × −1 | wrap Int64 `MIN` | raise long overflow | wrap bigint `MIN` |
| i32 add control | 1 + 1 as int32 | Int32 `2` | int `2` | int `2` |

### 4.3 What C-002 / C-003 take from this

- Same-width typed `INT`/`BIGINT` `+ − *` must become checked kernels that read
  `SparkAnsiConfig` (DEC U5 shape). `ansi=true` (default) raises Spark's
  `ARITHMETIC_OVERFLOW` needle. `ansi=false` wraps and keeps the Arrow type.
- `CAST(INT) + 1` and facade `col(int32) + 1` are silent widening on every
  repark door. C-002 forbids that on the Spark door: a bare integer literal
  that fits in `INT` beside an `Int32` operand must stay `Int32`, matching
  Spark's `fromLiteral`.
- Untyped `2147483647 + 1` is a **literal-width** split (repark Int64 vs Spark
  INT), named in Y-10 as "literals infer Int64". It is recorded here. C-002's
  "no silent widening" targets a typed INT column plus a Spark-INT literal,
  not a global retype of every SQL integer literal.
- The two doors share DataFusion `BinaryExpr` today, so they wrap together.
  A Spark-only rewrite would leave the ANSI door wrapping. C-003 requires the
  ANSI door to raise (standard SQL; knob absent defaults raise). Spark
  `ansi=false` wrap vs ANSI raise is then an INTENDED split and needs a
  `cross_door.rs` pin.

## 5. C-005 measurement (2026-08-30)

`REPARK_PERF_MEASURE=1 cargo test -p repark-functions --lib perf_measure_non_overflow_int32_add`:
checked integer add 522.8 ms vs DataFusion baseline 419.4 ms over 200 scalar collects
(ratio **1.25**). Not an order-of-magnitude regression.

`make verify` exit 0. Facade suite 3793 passed, 75 skipped. `make py-test` 459 passed.
`make audit` and `make workflows-lint` exit 0.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: f-y10-1-int-overflow
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        C-001 matrix from live Spark 4.1.2 and repark probes. C-002/C-003 pins raise and wrap
        per operator and door. C-004 registry/FNP-7b/STATUS. C-005 gates and 1.25x measurement.
      artifacts: [crates/repark-functions/src/integer_spark.rs, python/repark/tests/test_integer_overflow_parity.py]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Overflow boundaries MAX+1, MIN-1, MAX*2, MIN*-1, CAST+lit, facade col+col and col+1,
        null+1, non-overflow control, explicit BIGINT cast not narrowed.
      artifacts: [crates/repark-functions/src/integer_spark.rs, crates/repark-sql/tests/cross_door_int_overflow.rs]
    - id: AT-3
      status: ATTACKED
      evidence: >
        ANSI raise is DataFusion Execution ARITHMETIC_OVERFLOW; ansi=false wrapping path is the
        documented Spark non-ANSI result, not NULL.
      artifacts: [crates/repark-functions/src/integer_spark.rs]
    - id: AT-4
      status: N/A
      justification: kernels are per-row pure arithmetic with no shared mutable session state beyond the ANSI flag.
    - id: AT-5
      status: N/A
      justification: no auth, injection, or filesystem surface; SQL is planned through existing doors.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Arrow type is part of every pin (Int32 wrap vs Int64 widen). Explicit CAST AS BIGINT + 1
        stays Int64 2147483648.
      artifacts: [crates/repark-functions/src/integer_spark.rs]
    - id: AT-7
      status: ATTACKED
      evidence: >
        Non-overflow Int32 add measured at 1.25x DataFusion baseline over 200 collects. Not
        system-breaking.
      artifacts: [crates/repark-functions/src/integer_spark.rs]
    - id: AT-8
      status: ATTACKED
      evidence: >
        Error needle matches Spark ARITHMETIC_OVERFLOW / try_add|try_subtract|try_multiply /
        integer vs long overflow. Lit(int) that fits is Int32 like Spark IntegerType.
      artifacts: [crates/repark-python/src/column/mod.rs]
    - id: AT-9
      status: N/A
      justification: no new log/metric surface; failures are query-time Execution errors.
    - id: AT-10
      status: ATTACKED
      evidence: >
        Red-first C-001 matrix; pins name raise vs wrap vs widen; mutation of select alias still
        leaks Int32(1). Revert of checked rewrite would red the raise tests.
      artifacts: [python/repark/tests/test_select_naming.py, python/repark/tests/test_integer_overflow_parity.py]
  reattested: []
  complete: true
```
