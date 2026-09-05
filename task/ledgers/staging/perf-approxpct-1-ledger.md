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
| C-001 | The new sketch module reproduces Spark 4.1 `QuantileSummaries` insert / compress / merge / query semantics exactly (compressThreshold 10000, head 50000, relativeError 1/accuracy, the delta rule, the backwards merge test, Long-division targetError, the edge rules). | Rust unit tests in the new module: insert/compress invariants, merge associativity, query bounds. | OPEN | Which red each mutation trips: §5. |
| C-002 | `percentile_approx` / `approx_percentile` honour accuracy on the group-by path on both SQL doors and the DataFrame door, Spark-equal on the matrix (accuracy default/100/10/2 × scalar/array × NULLs × duplicate-heavy × skewed × int/float/decimal), measured on live PySpark 4.1.2. | `test_perf_approxpct_1.py` always-run pins + live legs. | OPEN | Goldens recorded from the pinned oracle; the banner is quoted in §4. |
| C-003 | The WIN-SLIDE-1 frame re-scan path honours accuracy per frame: x=1..200, 100-row frame, accuracy 2 answers Spark's `(1.0, 1.0, 1.0, 51.0, 101.0)`. | The flipped `test_win_slide_1.py` sketch pin + a matrix leg. | OPEN | The evaluator builds a fresh accumulator per frame; the sketch rides it unchanged. |
| C-004 | Memory is sketch-bounded and wall is within bar: peak RSS for `percentile_approx(x, 0.5)` over 1e7 rows in one group carries a ≤ few-MB state (fresh subprocess, release module), and wall at 1e6 rows is within 1.5× of the current kernel. | `docs/perf/approx-percentile-baseline.md` before/after tables + a structural pin. | OPEN | BEFORE measured on the base release module in §3. |
| C-005 | `FN-APPROXPCT-ACC-1`'s residue is re-measured and closed or narrowed honestly, and all three registry rows carry dates and the unit id. | The three flipped rows in `docs/spark-sql-iceberg-parity.md`. | OPEN | |
| C-006 | No regressions: the pre-existing percentile pins (default-accuracy discrete answers, the win-slide 65-cell matrix, the live legs) stay green; `make ci`, `make verify`, the facade suite, the parity suite, the live tier on the touched legs, and `make py-test-dbt` are green; the mutation score is run. | The gates + §5. | OPEN | Every dbt flip classified. |
| C-007 | Docs in lockstep: the baseline doc, the touched `map.md` files, the ledger grammar; `STATUS.md` and `briefs/next-sequence.md` untouched. | The gates. | OPEN | |

VERDICT: 7 clauses, 0 PROVEN, 7 OPEN, 0 REJECTED.

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

## 3. BEFORE numbers

Recorded here once the base release module finishes building.

## 4. Accuracy matrix

Recorded here once the live oracle session runs.

## 5. Mutation score

Recorded here once the implementation lands.

## 6. Limitations

Recorded here as measured.
