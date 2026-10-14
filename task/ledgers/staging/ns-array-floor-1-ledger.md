# Unit ledger — NS-ARRAY-FLOOR-1 · the pandas and Arrow doors, both sides, and where each refuses

**Scope (2026-10-14):** measurement only, on main `d96028ee`. No product
file changes: nothing under `crates/*/src` or `python/repark/src` moves.
The parked U-1 head is read-only input-side context (module `07cbe563`);
live Spark 4.1.2 is the oracle for the microsecond control and for the two
card-recorded output answers.

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
| C-002 | The live input control is recorded with the Spark banner. | The live run of the same recorder. | **PROVEN** | `input-cells.md` live run: banner spark 4.1.2 zone UTC; 18 cells, every one the verbatim pandas-missing refusal (§3). |
| C-003 | The output cells are recorded on main: nine doors return nanosecond arrays and columns under the four declarations, frame ticks and read-back ticks. | The output recorder plus its evidence. | **PROVEN** | `python/repark/tests/_record_ns_array_floor_1_output.py`; `task/wo/ns-array-floor-1/evidence/output-cells.md` plus JSON: 36 cells, 9 floor to microseconds with stored ticks equal to frame ticks, 27 refuse. |
| C-004 | The live output cells are recorded with the Spark banner. | The live run of the same recorder. | **PROVEN** | `output-cells.md` live run: banner spark 4.1.2 zone UTC; 36 cells, every one the verbatim pandas-missing refusal (§3). |
| C-005 | Every erroring input cell (none) and every floor-answering output cell names its raising or copying function with file and line, confirmed by traceback, in a door-to-site table ending in the one-line site count. | `sites.md` plus the tracebacks in the output JSON. | **PROVEN** | `task/wo/ns-array-floor-1/evidence/sites.md`: six sites in four facade files, every one output-side, two shared functions with their two callers each named. |

## 1. Counts

D1 input cells by door (answer / error):

| Door | Main | U-1 head | Live Spark control |
|---|---|---|---|
| grouped applyInPandas | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| cogroup applyInPandas | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| grouped applyInArrow | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| cogroup applyInArrow | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| mapInPandas | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| mapInArrow | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| scalar pandas_udf | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| grouped-aggregate pandas_udf | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |
| scalar arrow_udf | 10 / 0 | 10 / 0 | 0 / 2 env-blocked |

The seen dtype is the finding: on main the two U-1 shapes arrive as
microseconds, on the U-1 head as nanoseconds with every digit. The
microsecond control arrives as microseconds on both.

D2 output cells on main (9 doors by array and top level under the
timestamp and timestamp_ntz declarations):

| Outcome | Cells | Which |
|---|---|---|
| floor to microseconds, stored ticks equal frame ticks | 9 | grouped and cogroup applyInPandas under both array declarations; mapInPandas, scalar pandas_udf and grouped-aggregate pandas_udf under array timestamp_ntz; scalar pandas_udf and grouped-aggregate pandas_udf under array timestamp |
| refuse | 27 | every top-level nanosecond column on every door; every grouped and cogroup applyInArrow cell; every mapInArrow cell; mapInPandas except array timestamp_ntz; scalar arrow_udf on all four |

## 2. Site line

sites: 6 (python/repark/src/repark/spark/dataframe/grouped_arrow.py:103, python/repark/src/repark/spark/dataframe/grouped_arrow.py:271, python/repark/src/repark/spark/dataframe/udf_schema.py:60, python/repark/src/repark/spark/dataframe/udf_bridge.py:140, python/repark/src/repark/spark/functions_arrow_udf.py:121, python/repark/src/repark/spark/dataframe/udf_bridge.py:38)

No site serves the input side. The `:103` and `:271` functions each serve
two doors through two different callers above them.

## 3. What was not measured

- The dtype a live Spark user's function sees: the live module holds no
pandas, so every live door refuses before any function runs
(`PACKAGE_NOT_INSTALLED`, verbatim in the evidence). A hybrid run with
another interpreter's pandas was tried and rejected as oracle evidence.
- Live Spark answers for doors the card did not measure; only grouped and
cogroup applyInPandas array cells and the top-level control carry
card-recorded Spark answers.
- The U-1 head output side; non-UTC session zones; struct and map nesting;
write doors beyond the recorder's own append round-trip.
- No neighbour statement was re-run: the microsecond control (iv) is the
in-tree baseline for the next step, recorded on both builds.

## Q. Questions

- **Q1 (RULING).** The live dtype cells are env-blocked, not refused by
Spark. *Premise:* `/tmp/sparkenv` holds pyspark 4.1.2 and pyarrow but no
pandas, so every live door ends in `PACKAGE_NOT_INSTALLED` before any user
function runs; installing pandas there is outside this lane. *Lean:* accept
the lane's live evidence as the banner plus the verbatim block, with the
card's verdict-2 values as the live reference.

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
      evidence: NULLs, naive and aware nanoseconds, microseconds, top-level and array nesting, and the two U-1 expression shapes are all exercised on both builds.
      artifacts: [python/repark/tests/_record_ns_array_floor_1_input.py]
    - id: AT-3
      status: ATTACKED
      evidence: All 27 main refusals and all 54 live env-blocks carry the error class, the condition where Spark names one, the first line, and the file and line traceback.
      artifacts: [task/wo/ns-array-floor-1/evidence/output-cells.json]
    - id: AT-4
      status: N/A
      justification: Single-threaded recorders with no shared state; no product code moves, so no ordering or concurrency surface exists.
    - id: AT-5
      status: N/A
      justification: Memory catalogs and scratch warehouses only; no credentials, no network writes, no privileged action anywhere in the unit.
    - id: AT-6
      status: ATTACKED
      evidence: Every answering output cell compares read-back ticks against frame ticks; all nine agree, so no silent store-side change hides behind the bridge.
      artifacts: [task/wo/ns-array-floor-1/evidence/output-cells.json]
    - id: AT-7
      status: N/A
      justification: No performance claim is made; the matrices are 198 and 72 small cells run under a bounded address-space cap.
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
      justification: No product behavior changes, so no regression pin applies; the two recorders re-derive every cell on demand and carry the clause citations.
  complete: true
```
