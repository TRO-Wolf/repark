# Charter ledger — DFCORE-5 · `approxQuantile` in one collect per frame

**Date:** 2026-09-07 · **Branch:** `perf/dfcore-5` · **Base:** `origin/main`
`980eb76d` (PR #419, DFCORE-4b) · **Model:** muse-spark-1.3 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `PERF-APPROXQUANTILE-1` **FIXED**.

**Retires:** this ledger moves to `../completed/` in this unit's last commit.

**Why now.** The decomposition plan
(`task/roadmap/epic-term/dataframe-core-decomposition-plan-2026-09-07.md`, §4 row DFCORE-5)
opens the first behaviour unit of the slate: `approxQuantile` runs one
`collect()` per column per probability (six for two columns by three
probabilities — the plan's §1 C-004 and DFCORE-3's recorded baseline) while
`percentile_approx` already accepts a probability list. This unit batches the
loop to one collect per frame and closes the three pin gaps the DFCORE-3 and
DFCORE-4a critics left (crosstab absent-pair 0, freqItems full text, sample
fraction full text).

**Not in this unit:** honoring `relativeError` (it stays validated and
IGNORED — changing that is not this unit); converging the three pre-existing
oracle divergences §4 measures (empty-probabilities NPE, NULL/empty `[]`,
non-numeric error class); `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.lock`, dependency lists, completed ledgers, any move.

**Environment.** The lane native arrived stale (`approx_percentile_cont`
arity, the DFCORE-3 R2-S2 class) and debug; the unit rebuilt it release
(`uvx maturin@1.14.1 develop --release`, `CARGO_BUILD_JOBS=8`) before
measuring. All numbers below are release with `__debug_assertions__` False.

## PROPOSITION LEDGER — DFCORE-5 — 2026-09-07

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Every non-trivial shape collects exactly once per frame; empty shapes collect never (2x3 6→1, 4x5 20→1, single 3→1, 1x1 1→1, empties 0→0). | `test_one_collect_per_frame_regardless_of_shape` (autospec count pin). | **PROVEN** | Pin green after; red pre-fix (stash run: 6 vs 1). Wall medians recorded in §3, not gated. pins: dfcore-5/C-001 |
| C-002 | Every preserved shape answers identically on the `df.` and `df.stat.` doors: validation order with exact classes and parameters, empty probs/cols, flat single, dupes, NULL ignored, all-NULL/empty NaN, int/decimal/float, non-numeric engine error, nested float shape, ignored relativeError. | The value/contract pins in `test_dfcore_5_approx_quantile.py`. | **PROVEN** | 13 pins green; the nesting mutation reds 6 of them; 1e6 values identical before/after (§3). pins: dfcore-5/C-002 |
| C-003 | Live PySpark 4.1.2 answers the audit values on both doors, and the all-NULL/empty divergence is pinned on both sides (repark NaN per probability, Spark `[]`). | The two live legs under `REPARK_PARITY_LIVE=1`. | **PROVEN** | 19 passed with the tier armed (2 live legs green); goldens in §4. pins: dfcore-5/C-003 |
| C-004 | The three inherited gaps close: a crosstab pin with an absent pair asserting the `0` fill; a freqItems pin matching the full message; a sample pin matching the full fraction text. | The three gap pins in `test_dfcore_5_approx_quantile.py`. | **PROVEN** | Text mutations red both message pins; the crosstab pin reds on a moved cell (§5); the fill-drop stays green because the pivot already yields 0 (§6). pins: dfcore-5/C-004 |
| C-005 | Registry row FIXED with before/after counts and medians; baseline section dated with numbers and commands; ledger, staging map, and every touched map in lockstep; `statistics.py` under the default ceiling; gates green. | The gates + §7. | **PROVEN** | `PERF-APPROXQUANTILE-1` FIXED; §"DataFrame.approxQuantile" appended; `statistics.py` 261→264, no row; battery 277 passed + 6 skipped; facade 5791 passed + 356 skipped. pins: dfcore-5/C-005 |

VERDICT: 5 clauses, 5 PROVEN, 0 OPEN, 0 REJECTED.

## 1. Scope audit

The brief's five steps plus the plan's DFCORE-5 row (§4) and unit-specific
pins (§5: all-null and empty columns, empty probability list, duplicate
columns, integer and decimal inputs, result shape, before/after collect
count) are the charter. The plan's §1 C-004 is the defect statement; the
DFCORE-3 ledger's collect-count probe (6 before, 6 after the move) is the
pre-fix baseline this unit must halve. The DFCORE-3 critic report (F1
crosstab fill, F2 freqItems text) and the DFCORE-4a critic report (F1 sample
fraction text) name the three gap pins. No clause needed killing: every
step reduces to a checkable proposition above.

## 2. Design

One aggregation projects the list form of `percentile_approx` per column
under positional aliases (`_q0`, `_q1`, …) and unpacks per column; a single
`collect()` answers the frame. Empty probs/cols short-circuit with no
collect (the old loop never reached the engine either). A NULL cell answers
NaN per probability; a NULL list element would too (the old scalar rule,
generalized). The validation block, the wrapper docstrings, and the return
shape are untouched. Two measured facts shaped the design: the engine
reports the first failing column in multi-aggregate order, so mixed
good/bad and bad/bad column lists raise exactly the old sequential errors;
and the list form answers NULL (not a NULL list) on empty and all-NULL
input, so the NaN mapping sits on the unpack, not the query. Reasons live
in the touched `map.md` files.

## 3. Before/after numbers

Release module, one session per run, frame `range(1, 1000001)` with `x = id`
and `y = id * 2` (`z = x + 1`, `w = y + 1` for the 4-column shape). Collect
counts via a monkeypatched counter; wall cells are five-run medians with the
1-minute load beside each run (throwaway `/tmp/dfcore5-before.py` and
`/tmp/dfcore5-bench.py`).

| shape | before collects | after collects | before median | after median |
|---|---|---:|---:|---:|
| 2 cols x 3 probs | 6 | 1 | 0.155 s (load 8.4) | 0.052 s (load 6.5) |
| 1 col x 1 prob | 1 | 1 | 0.028 s (load 8.4) | 0.028 s (load 6.5) |
| 4 cols x 5 probs | 20 | 1 | 0.572 s (load 9.8) | 0.087 s (load 6.5) |
| empty probs / empty cols | 0 | 0 | — | — |

The 1x1 shape is unchanged at one collect, as designed. The loads differ
between runs, so no wall ratio is claimed — counts are exact, wall is
recorded. Values are identical before and after at 1e6 rows (2x3
`[[249924.0, 499971.0, 750030.0], [499848.0, 999942.0, 1500060.0]]`), and the
audit frame answers `[[250, 500, 750], [500, 1000, 1500]]` on both doors
before and after.
pins: dfcore-5/C-001

## 4. Live oracle goldens

Oracle: live PySpark 4.1.2, ANSI on, UTC, `local[2]`
(`JAVA_HOME=/usr/lib/jvm/zulu-17-amd64`, `SPARK_LOCAL_IP=127.0.0.1`,
`PYSPARK_SUBMIT_ARGS` pointing `spark.jars.ivy` at the lane `.ivy2` seed;
three JVM runs, each stopped, none lingering). Agreement cells: the audit
frame `[[250.0, 500.0, 750.0], [500.0, 1000.0, 1500.0]]` on both doors and
both shapes, empty cols `[]`, dupes `[[500.0], [500.0]]`, NULL cells `[1.0]`
and `[1.0, 1.0, 3.0]`, int `[50.0]`, decimal `[2.2]` — all equal on both
engines. Divergence cells (pre-existing, preserved, pinned in the live leg):

- All-NULL column / empty frame: Spark answers `[]` (single) and `[[]]`
  (one-column list; mixed `["x", "n"]` answers `[[1.0], []]`) where repark
  answers NaN per probability. The brief's "Spark answers NaN" premise is
  wrong on 4.1.2; the head clause (preserve exactly) governs, so repark
  keeps NaN and the live leg pins both sides.
- Empty probabilities: Spark raises `NullPointerException`
  (`DataFrameStatFunctions.scala:51`, `Seq.toArray` on null) where repark
  answers `[]` per column with no collect.
- Non-numeric column: Spark raises `IllegalArgumentException: requirement
  failed: Quantile calculation for column s with data type StringType is not
  supported.` where repark raises `PySparkException: datafusion engine
  error: Execution error: percentile_approx does not support Utf8 values`.

## 5. Mutation score

5/6 red, each mutant applied, run, and reverted with the restore verified
by `diff -q` against a pristine copy:

- Pre-fix tree (fix stashed): the count pin reds (`6 vs 1`).
- Nesting (`return results[0] if single else results` → `return results`):
  6 pins red.
- freqItems text (`R-DF-BATCH2` → `R-DF-BATCH2-MUT`): the full-message pin reds.
- Sample fraction text (`[0, 1]` → `[0, 1] MUT`): the full-message pin reds.
- Crosstab absent cell forced to 9: the sparse pin reds.
- Crosstab fill dropped (`pivoted.na.fill(0)` → `pivoted`): stays green —
  the pivot already yields int64-not-null 0 for absent pairs, so the fill is
  defensive (§6). The pin locks the 0 contract, not the fill call.
  pins: dfcore-5/C-004

## 6. Limitations

- `relativeError` is validated and IGNORED — the unit preserves that
  contract and pins it; honoring the knob is not this unit.
- The three §4 divergences (empty-probabilities NPE, NULL/empty `[]`,
  non-numeric error class) are pre-existing and preserved; converging any
  of them is a follow-up unit with its own registry disposition, not a
  passenger here.
- The crosstab `.na.fill(0)` is defensive: `pivot().count()` already emits
  0 for absent pairs. The new pin locks the value, so removing the fill
  stays safe only while the pin is green.
- The two `approxQuantile` docstrings still name the scalar
  `approx_percentile_cont` lowering. They stay byte-identical under the
  unit's never-reword rule; the list-form truth lives in
  `dataframe/map.md` and §2 above.

## 7. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: dfcore-5
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Clauses C-001..C-005 walked one by one against behavior; the verdict table carries the evidence per clause, and every clause is cited from the tests map.
      artifacts: [task/ledgers/staging/dfcore-5-ledger.md, python/repark/tests/test_dfcore_5_approx_quantile.py]
    - id: AT-2
      status: ATTACKED
      evidence: Empty probs/cols, single flat, dupes, NULL ignored, all-NULL, empty frame, int/decimal/float, non-numeric, and both doors all exercised; the pin file reports 17 passed + 2 skipped JVM-free.
      artifacts: [python/repark/tests/test_dfcore_5_approx_quantile.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every validation branch (type before value, exact classes and parameters) and every engine error (Utf8, Boolean, missing field, first-failing-column order) pinned on both doors.
      artifacts: [python/repark/tests/test_dfcore_5_approx_quantile.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state, no ordering change, no async spawn, no lock; one synchronous aggregation replaces N synchronous aggregations.
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no trust-boundary crossing; the live tier runs merged-code-only under its own gate.
    - id: AT-6
      status: ATTACKED
      evidence: Both public doors keep signatures, shapes, and error contracts; int/decimal answers still arrive as floats; the three oracle divergences are pinned, not papered over, by the live legs (19 passed with the tier armed).
      artifacts: [python/repark/tests/test_dfcore_5_approx_quantile.py, task/ledgers/staging/dfcore-5-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: The unit is the AT-7 fix: collects 6 to 1 (2x3) and 20 to 1 (4x5), before/after medians recorded with loads, values identical; the count pin carries no wall gate by charter.
      artifacts: [docs/perf/approx-percentile-baseline.md, python/repark/tests/test_dfcore_5_approx_quantile.py]
    - id: AT-8
      status: ATTACKED
      evidence: Facade-only change in one leaf module; ceilings and maps current; the battery reports 277 passed + 6 skipped and the facade suite reports 5791 passed + 356 skipped.
      artifacts: [python/repark/src/repark/spark/dataframe/statistics.py, python/repark/tests/test_dfcore_5_approx_quantile.py]
    - id: AT-9
      status: N/A
      justification: No new log or metric surface; every failure path raises the same loud typed error as before.
    - id: AT-10
      status: ATTACKED
      evidence: Mutation score 5/6 with each mutant verified present and each restore verified by diff; the one green (fill-drop) is explained by the pivot's own 0 in section 6.
      artifacts: [task/ledgers/staging/dfcore-5-ledger.md]
```
