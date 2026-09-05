# Charter ledger — PERF-APPROXPCT-1 · the Greenwald-Khanna sketch behind `percentile_approx`

**Date:** 2026-09-05 · **Branch:** `perf/approxpct-1` · **Base:** `origin/main`
`bc7c76cc` · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `PERF-APPROXPCT-1`, `FN-APPROXPCT-ACC-1`, `WIN-SLIDE-PCT-ACC-1`.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** FN-FIX-1 ships `percentile_approx` / `approx_percentile` as a
whole-group buffer with the accuracy knob accepted and ignored; WIN-SLIDE-1
measures the per-frame residue (`(1.0, 25.0, 50.0, 100.0, 150.0)` vs Spark's
`(1.0, 1.0, 1.0, 51.0, 101.0)` at accuracy 2). Spark 4.1 answers from a
Greenwald-Khanna `QuantileSummaries` sketch, so the knob moves the answers
and bounds the memory. This unit ports the sketch semantics exactly and wires
the knob through on every door.

**Not in this unit:** a `GroupsAccumulator` (the aggregate serves none, so
the brief's conditional does not fire); temporal and string inputs (loud
`exec_err`, Spark parity for numerics/decimal only); decimal precision above
the f64 round-trip (noted in §6); any DataFusion fork change.

## PROPOSITION LEDGER — PERF-APPROXPCT-1 — 2026-09-05

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The new sketch module reproduces Spark 4.1 `QuantileSummaries` insert / compress / merge / query semantics exactly (compressThreshold 10000, head 50000, relativeError 1/accuracy, the delta rule, the backwards merge test, Long-division targetError, the edge rules). | Rust unit tests in the new module: insert/compress invariants, merge associativity, query bounds. | PROVEN | `cargo test -p repark-functions`: 15 sketch + 8 accumulator tests green; single-partition end-to-end agrees with Spark exactly (§4). pins: perf-approxpct-1/C-001 |
| C-002 | `percentile_approx` / `approx_percentile` honour accuracy on the group-by path on the SQL door and the DataFrame door, Spark-equal on the matrix (accuracy default/100/10/2 × scalar/array × NULLs × duplicate-heavy × skewed × int/float/decimal), measured on live PySpark 4.1.2. | `test_perf_approxpct_1.py` always-run pins + live legs. | OPEN | 56 always-run legs green on the debug module; the live legs run in the gates. pins: perf-approxpct-1/C-002 |
| C-003 | The WIN-SLIDE-1 frame re-scan path honours accuracy per frame: x=1..200, 100-row frame, accuracy 2 answers Spark's `(1.0, 1.0, 1.0, 51.0, 101.0)`. | The flipped `test_win_slide_1.py` sketch pin + a matrix leg. | OPEN | The evaluator builds a fresh accumulator per frame; the sketch rides it unchanged. |
| C-004 | Memory is sketch-bounded and wall is within bar: peak RSS for `percentile_approx(x, 0.5)` over 1e7 rows in one group carries a ≤ few-MB state (fresh subprocess, release module), and wall at 1e6 rows is within 1.5× of the current kernel. | `docs/perf/approx-percentile-baseline.md` before/after tables + a structural pin. | OPEN | BEFORE measured on the base release module in §3. |
| C-005 | `FN-APPROXPCT-ACC-1`'s residue is re-measured and closed or narrowed honestly, and all three registry rows carry dates and the unit id. | The three flipped rows in `docs/spark-sql-iceberg-parity.md`. | OPEN | |
| C-006 | No regressions: the pre-existing percentile pins (default-accuracy discrete answers, the win-slide 65-cell matrix, the live legs) stay green; `make ci`, `make verify`, the facade suite, the parity suite, the live tier on the touched legs, and `make py-test-dbt` are green; the mutation score is run. | The gates + §5. | OPEN | Every dbt flip classified. |
| C-007 | Docs in lockstep: the baseline doc, the touched `map.md` files, the ledger grammar; `STATUS.md` and `briefs/next-sequence.md` untouched. | The gates. | OPEN | |

VERDICT: 7 clauses, 1 PROVEN, 6 OPEN, 0 REJECTED.

## 1. Scope audit

The brief's three registry rows plus the accuracy matrix in §4 are the
charter. The sketch source is Spark v4.1.2 `QuantileSummaries.scala` (391
lines) and `ApproximatePercentile.scala` (383 lines), read in full; the
round-trip rule for decimals is `Decimal.set(Double)` via the shortest-repr
`BigDecimal(double)`, read in `Decimal.scala`.

## 2. Design

One new module holds the sketch; `percentile_approx.rs` keeps its UDAF shell
and swaps the `Vec<ScalarValue>` buffer for the summary. State becomes the
serialized summary plus the percentages list. The facade stops deleting
`accuracy` and the binding threads it as a third literal. The window
re-scan evaluator already builds a fresh accumulator per frame, so C-003
needs no evaluator change. Reasons live in the touched `map.md` files.

Two routing facts shaped the DataFrame door. The list form always lowers through the
global-aggregate SQL path (nested parens fail the native classifier), so the facade
`sql_expr` carries the accuracy tail or the knob silently drops on that path; the scalar
form rides the native `_inner` expr. Multi-partition merge trees differ legitimately
between engines (the same UNION text answered 106.0 then 110.0 on consecutive runs), so
scan-path matrix cells run on one-partition `repartition(1)` views, where both engines
agree exactly; edge-rule and discrete cells stay as UNION text.

## 3. BEFORE numbers

Base release module (`debug=False`), whole-group `Vec<ScalarValue>` kernel,
`/tmp/bench_approx.py` (`range(1, N+1)` → `percentile_approx(id, 0.5)`,
fresh subprocess, 1-minute load beside each cell):

| cell | wall | peak RSS | load |
|---|---|---|---|
| 1e6, attempt 0 (cold session) | 1.28 s | 475.7 MB | 30.3 |
| 1e6, attempt 1 (warm) | 0.21 s | 644.9 MB | 30.3 |
| 1e6, attempt 2 (warm) | 0.13 s | 697.7 MB | 30.3 |
| 1e7, fresh subprocess | 2.95 s | 2507.8 MB | 25.5 |

Answers 500000 / 5000000, both correct. The 1e7 state is ~2.5 GB for a
one-group sketchable aggregate; the AFTER column must carry megabytes. Wall
bar: warm AFTER at 1e6 within 1.5× of warm BEFORE (~0.13–0.21 s at load
~30; the AFTER run re-records its own load and repeats).

## 4. Accuracy matrix

Oracle: live PySpark 4.1.2, ANSI on, UTC (`BANNER spark=4.1.2 tz=UTC`,
2026-09-05T22:11Z and 22:25Z, `local[2]`). Probe 1 used bare `1.0` literals,
which Spark reads as DECIMAL, not DOUBLE — those cells stand as the decimal
goldens and probe 2 re-measured every cell with explicit CASTs.

Group-by p50 over x=1..200 (double, int and decimal agree numerically):

| accuracy | default | 100 | 10 | 2 |
|---|---|---|---|---|
| double | 100.0 | 99.0 | 98.0 | 1.0 |
| int | 100 | 99 | 98 | 1 |
| decimal(10,2) 1.25..200.25 | 100.25 | 99.25 | 98.25 | 1.25 |

Scan-path cells (not edge rules): p25/acc10 64.0, p75/acc10 152.0,
array[0.25,0.5,0.75]/acc10 [64.0,98.0,152.0]; dupes
(500×5, 250×1, 250×9) array/acc10 [1.0,5.0,5.0]. NULLs [1,2,3,NULL,4,6]:
default 3.0, acc2 1.0. Grouped (a:[1,2,3], b:[4,6]): default (2,4), acc10
(2,4), acc2 (1,4); grouped array/acc2 a:[1,1,3] b:[4,4,6]. Skew
(1..1000 + 1e9): default 501.0, acc2 1.0, p99/acc2 1e9. Fractions
[0.1..0.9]: default 0.3, acc2 0.1. Decimal(10,2) five-row: default 3.30,
acc2 1.10. Same-value 1000×7.0: 7.0 at both. Negatives [-5,-1,0,0,3]:
default 0.0, acc2 -5.0. Float: default 2.5 (float), acc2 1.5. Alias
`approx_percentile` answers identically.

Surprises, both reproduced in the pins: a NULL array element reads as 0.0
(`array(0.5, NULL-double)` answers `[100.0, 1.0]`, the second cell is the
p0.0 minimum — Spark's `toDoubleArray` unboxes null to 0.0); `array()`
answers NULL. Empty input and all-NULL input answer NULL. Date/timestamp
inputs answer the discrete minimum of the two-row probe. String input does
not reach the sketch: Spark's implicit cast fails first
(`CAST_INVALID_INPUT` on 'b' under ANSI).

Spark error contracts (analysis time; repark raises at execution setup):
accuracy 0/-3/NULL/2147483648 →
`DATATYPE_MISMATCH.VALUE_OUT_OF_RANGE` / `UNEXPECTED_NULL`, range
`(0, 2147483647]`; percentage 1.5/-0.5/NULL → `VALUE_OUT_OF_RANGE` /
`UNEXPECTED_NULL`, range `[0.0, 1.0]`.

Frame cells (x=1..200 double, 100-row frame, rows 1/50/100/150/200):
default `(1.0, 25.0, 50.0, 100.0, 150.0)`, acc10
`(1.0, 23.0, 48.0, 98.0, 148.0)`, acc2 `(1.0, 1.0, 1.0, 51.0, 101.0)` —
the acc2 column reproduces the WIN-SLIDE-1 measurement exactly.

## 5. Mutation score

Recorded here once the implementation lands.

## 6. Limitations

- The native ANSI door (`repark.sql()`) does not resolve `percentile_approx`
  at all — `Invalid function 'percentile_approx'. Did you mean
  'percentile_cont'?` — pre-existing (FN-FIX-1 shipped Spark-door-only) and
  out of scope: registering a Spark-dialect aggregate on the ANSI door is a
  product decision (Trino's `approx_percentile` interpolates), not a perf
  unit's passenger. "Both doors" in this unit is the SQL door plus the
  DataFrame door, the WIN-SLIDE-1 C-001/C-002 split.
- String input: Spark implicit-casts to DOUBLE first (a numeric string works
  there); repark raises a loud type error. Not in the brief's matrix; filed
  here, not papered over.
- NaN values: sort/order follows `total_cmp` plus IEEE comparisons; Spark's
  `sorted` (IeeeOrdering) may order NaNs differently. No measured cell
  contains NaN.
- Decimal precision above the f64 round-trip: the sketch stores doubles, so
  a decimal whose shortest-repr double needs more than `scale` fractional
  digits is quantized HALF_UP to `scale` where Spark keeps the full repr.
  All measured decimal cells round-trip exactly.
- Scientific literals: repark parses `1e9` as `decimal128(1, -9)` where Spark
  reads a double, and the cast loses a digit (`999999999.9999999`). Pre-existing,
  out of scope; the skew fixture uses an integer literal.
- Facade accuracy edges: `bool`/`numpy` accuracy reaches the native path as an
  int (lenient) while the SQL tail renders non-`int` as NULL (loud UDAF error).
  Spark rejects both. Filed, not papered over.
- The WIN-SLIDE-1 staging ledger's C-006 prose ("ignores the accuracy knob") goes
  stale the moment this unit lands its flip; that unit trues up its own ledger,
  and the hand-back flags it.
