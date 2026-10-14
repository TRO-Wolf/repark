# Unit ledger — NS-ARRAY-FLOOR-1 · the pandas and Arrow doors, both sides, and where each refuses

**Scope (2026-10-14):** measurement only, on main `d96028ee`. No product
file changes: nothing under `crates/*/src` or `python/repark/src` moves.
The parked U-1 head is read-only input-side context (module `07cbe563`).
Follow-up 1 re-runs the live columns on the working live module under both
values of `spark.sql.execution.pandas.convertToArrowArraySafely`, adds
floor-vs-truncation values, and records the Lane A echo shape.

**Date:** 2026-10-14 · **Branch:** `fix/ns-array-floor-1` · **Base:** `d96028ee` ·
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Order:** the step-0 brief NS-ARRAY-FLOOR-1 (U-2, R-020), the card
[ns-array-floor-1-card-2026-10-13.md](../../roadmap/mid-term/ns-array-floor-1-card-2026-10-13.md),
parity row ICE-TSNS-SQL-1-R-020. The facade has the grouped-aggregate
`pandas_udf`, so all nine doors are measured.
**This ledger closes when the unit's pull request merges.**

## PROPOSITION LEDGER — NS-ARRAY-FLOOR-1 — 2026-10-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The input cells are recorded on main and on the U-1 head: nine doors over five sources at top level and inside an array. | The input recorder plus its evidence. | **PROVEN** | `python/repark/tests/_record_ns_array_floor_1_input.py`; `task/wo/ns-array-floor-1/evidence/input-cells.md` plus JSON: 90 main cells and 90 U-1 cells, every one an answer. |
| C-002 | The live input control is recorded with the Spark banner under both safe settings. | The live runs of the same recorder. | **PROVEN** | `input-cells.md` live runs: banner spark 4.1.2 zone UTC, pandas 3.0.6; 18 cells per setting, every one an answer, with the per-level dtype diagnostics. |
| C-003 | The output cells are recorded on main: nine doors return nanosecond arrays and columns under the four declarations, frame ticks and read-back ticks. | The output recorder plus its evidence. | **PROVEN** | `python/repark/tests/_record_ns_array_floor_1_output.py`; `task/wo/ns-array-floor-1/evidence/output-cells.md` plus JSON: 36 cells, 9 floor to microseconds with stored ticks equal to frame ticks, 27 refuse. |
| C-004 | The live output cells are recorded with the Spark banner under both safe settings. | The live runs of the same recorder. | **PROVEN** | `output-cells.md` live runs: 36 cells per setting; safe=false answers 20 with floor ticks on arrays and truncation ticks on top-level columns; safe=true answers 10 array floors and refuses 26 with the worker PySparkValueError. |
| C-005 | Every erroring input cell (none) and every floor-answering output cell names its raising or copying function with file and line, confirmed by traceback, in a door-to-site table ending in the one-line site count. | `sites.md` plus the tracebacks in the output JSON. | **PROVEN** | `task/wo/ns-array-floor-1/evidence/sites.md`: six sites in four facade files, every one output-side, two shared functions with their two callers each named. |
| C-006 | The Lane A echo shape is recorded on main, the U-1 head, and live Spark. | The echo recorder plus its evidence. | **PROVEN** | `python/repark/tests/_record_ns_array_floor_1_lane_a.py`; `task/wo/ns-array-floor-1/evidence/lane-a-cells.md` plus JSON: 24 main cells (16 answer, 8 refuse), 24 U-1 cells (8 answer, 16 refuse), 16 live cells (16 answer). |
| C-007 | `sites.md` is restated from the measured Spark columns with the per-door same-or-differs table and the rule-per-site line. | `sites.md` against the output JSON. | **PROVEN** | Main against Spark at the default safe setting is same on 35 of 36 cells with equal ticks; the one differs is mapInPandas under array timestamp. |

## 1. Counts

D1 input cells by door (answer / error), 216 cells:

| Door | Main | U-1 head | Live safe=false | Live safe=true |
|---|---|---|---|---|
| grouped applyInPandas | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| cogroup applyInPandas | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| grouped applyInArrow | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| cogroup applyInArrow | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| mapInPandas | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| mapInArrow | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| scalar pandas_udf | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| grouped-aggregate pandas_udf | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |
| scalar arrow_udf | 10 / 0 | 10 / 0 | 2 / 0 | 2 / 0 |

The seen dtype is the finding: on main the two U-1 shapes arrive as
microseconds, on the U-1 head as nanoseconds with every digit. The
microsecond control arrives as microseconds on both. On live Spark a
microsecond TIMESTAMP arrives as `datetime64[ns]` in pandas and as
`timestamp[us, tz=UTC]` in Arrow batches, under both safe settings.

D2 output cells (9 doors by array and top level under the timestamp and
timestamp_ntz declarations), 108 cells:

| Run | Answer | Refuse |
|---|---|---|
| main | 9, every one a floor with stored ticks equal to frame ticks | 27 |
| live safe=false | 20: arrays floor, top-level columns truncate toward zero | 16, the four Arrow doors |
| live safe=true | 10 array floors | 26 with the worker PySparkValueError |

Main floors on the same cells Spark floors: every joint answer stores
equal ticks. The pre-epoch values separate the rules: `-1` ns stores `-1`
by floor and `0` by truncation; `-1500` ns stores `-2` by floor and `-1`
by truncation.

F2 echo cells (the function returns its input frame unchanged under the
frame's own schema), 64 cells:

| Run | Answer | Refuse |
|---|---|---|
| main | 16, the narrowed coal shape and the control | 8, the plain nanosecond column on every door |
| U-1 head | 8, the control only | 16, the plain and the widened nanosecond columns |
| live Spark | 16, control and ns_pandas frames | 0 |

The live ns_pandas frame stores floor ticks shifted by the JVM zone
(January dates take EST, plus 18000000000); its input reads
`timestamp[us, tz=UTC]`.

## 2. Site line

sites: 6 (python/repark/src/repark/spark/dataframe/grouped_arrow.py:103, python/repark/src/repark/spark/dataframe/grouped_arrow.py:271, python/repark/src/repark/spark/dataframe/udf_schema.py:60, python/repark/src/repark/spark/dataframe/udf_bridge.py:140, python/repark/src/repark/spark/functions_arrow_udf.py:121, python/repark/src/repark/spark/dataframe/udf_bridge.py:38)

No site serves the input side. The `:103` and `:271` functions each serve
two doors through two different callers above them.

## 3. What was not measured

- The U-1 head output side; non-UTC session zones; struct and map nesting;
write doors beyond the recorder's own append round-trip; the echo shape
through grouped-aggregate `pandas_udf`, which cannot return a frame.
- The card's verdict-2 refusal under `array<timestamp_ntz>` does not
reproduce on pandas 3.0.6: Spark floors there under both safe settings.
- No neighbour statement was re-run: the microsecond control is the
in-tree baseline for the next step, recorded on all three builds.

## Q. Questions

- **Q1 (RULING, ANSWERED by follow-up 1).** The first live module held no
pandas, so every live door ended before any user function ran. *Premise:*
the named live module could not run a pandas door. *Answer:* the working
live module re-ran every live column under both safe settings; the
env-blocked runs are superseded and dropped from the evidence.

## 4. Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ns-array-floor-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every brief clause is measured in the evidence files and named in the clause table above; no clause is argued from code reading alone.
      artifacts: [task/wo/ns-array-floor-1/evidence/input-cells.json, task/wo/ns-array-floor-1/evidence/output-cells.json]
    - id: AT-2
      status: ATTACKED
      evidence: NULLs, naive and aware nanoseconds, microseconds, pre-epoch floor-vs-truncation values, top-level and array nesting, the two U-1 expression shapes, and the echo schema shapes are all exercised on all three builds.
      artifacts: [python/repark/tests/_record_ns_array_floor_1_input.py, python/repark/tests/_record_ns_array_floor_1_lane_a.py]
    - id: AT-3
      status: ATTACKED
      evidence: All 27 main refusals, all 42 live refusals, and all 24 echo refusals carry the error class, the condition where Spark names one, the first and innermost lines, and the file and line traceback.
      artifacts: [task/wo/ns-array-floor-1/evidence/output-cells.json, task/wo/ns-array-floor-1/evidence/lane-a-cells.json]
    - id: AT-4
      status: N/A
      justification: Single-threaded recorders with no shared state; no product code moves, so no ordering or concurrency surface exists.
    - id: AT-5
      status: N/A
      justification: Memory catalogs and scratch warehouses only; no credentials, no network writes, no privileged action anywhere in the unit.
    - id: AT-6
      status: ATTACKED
      evidence: Every answering main cell compares read-back ticks against frame ticks and agrees; every joint Spark answer stores equal ticks, read as integers through an Arrow cast.
      artifacts: [task/wo/ns-array-floor-1/evidence/output-cells.json]
    - id: AT-7
      status: N/A
      justification: No performance claim is made; the matrices are 216, 108, and 64 small cells run under a bounded address-space cap.
    - id: AT-8
      status: ATTACKED
      evidence: Every refusal is recorded verbatim with its Spark condition where one exists; card-recorded Spark values are labeled as card-recorded, never as lane measurements.
      artifacts: [task/wo/ns-array-floor-1/evidence/sites.md]
    - id: AT-9
      status: ATTACKED
      evidence: Every measured failure is diagnosable from the committed record: class, first line, and file and line traceback per cell, plus the door-to-site table.
      artifacts: [task/wo/ns-array-floor-1/evidence/sites.md]
    - id: AT-10
      status: N/A
      justification: No product behavior changes, so no regression pin applies; the three recorders re-derive every cell on demand and carry the clause citations.
  complete: true
```
