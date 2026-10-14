# Unit ledger — ICE-TSNS-FLOOR-DOORS-1 · step 0 measures main, finds the seam

**Date:** 2026-10-14 · **Branch:** `fix/ice-tsns-floor-doors-1` · **Base:** `018d0efd` ·
**Model:** Muse Spark (muse-spark-1.3-contributor) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** Target: the next bug release (1.5.6).

**Order:** [../../../task/wo/ice-tsns-floor-doors-1.md](../../../task/wo/ice-tsns-floor-doors-1.md)
(U-3, R-009) from registry row ICE-TSNS-SQL-1-R-009 and ledger question Q2 of
`ice-tsns-merge-wall-1-ledger.md` (claim C-014). **This ledger closes when the unit's pull request merges.**
Step 0 changes no product file and decides nothing.

**Why.** Into a microsecond `TIMESTAMP` column, INSERT floors a nanosecond
source and reads a naive wall in the session zone, while the overwrite doors
truncate toward zero and read the wall as UTC; into `TIMESTAMP_NTZ` every
door truncates. Step 0 re-records the control cells on main's release module
with the rule-derived references beside each observation, and names the seam
where the narrowing is decided.

## PROPOSITION LEDGER — ICE-TSNS-FLOOR-DOORS-1 — 2026-10-14

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The door matrix on main is recorded: 216 combinations, 2376 ticks, with harness-derived floor and session-zone references beside each observation. | The recorder and its evidence files. | **PROVEN** | `python/repark/tests/_record_ice_tsns_floor_doors_1_main.py`; `task/wo/ice-tsns-floor-doors-1/evidence/main-cells.json` (216 cells) and `main-cells.md` (counts by door and target). pins: ice-tsns-floor-doors-1/C-001 |
| C-002 | On `TIMESTAMP` targets the append class equals INSERT's floor on every tick, and the four overwrite doors differ wherever truncation or the UTC reading differs. | The count table of `main-cells.md`. | **PROVEN** | `main-cells.md`: INSERT, MERGE update arm, UPDATE on v3, `writeTo().append()`, `insertInto` append 66 of 66 EQUAL per version; INSERT OVERWRITE, MERGE insert arm, `overwritePartitions`, `insertInto(overwrite=True)` 28 EQUAL, 16 truncated, 14 wall-as-UTC, 8 both per version. pins: ice-tsns-floor-doors-1/C-002 |
| C-003 | On `TIMESTAMP_NTZ` targets every door, INSERT included, truncates toward zero. | The count table of `main-cells.md`. | **PROVEN** | `main-cells.md`: every door except v2 UPDATE 42 EQUAL, 21 truncated, 3 other per version; the other trio is the Kolkata `timestamptz_ns` pre-epoch set (truncation before the zone shift). pins: ice-tsns-floor-doors-1/C-003 |
| C-004 | The registry row's 72 cells reproduce on main. | The collapsed overlap of `main-cells.md`. | **PROVEN** | `main-cells.md`: the 48 collapsed combinations (4 doors x 2 sources x 3 zones x 2 versions; the 3 MERGE spellings collapse to one arm) show INSERT all EQUAL and every overwrite door differing; the split holds. pins: ice-tsns-floor-doors-1/C-004 |
| C-005 | The seam list names each door's narrowing expression and wall-localisation site with file and line, cross-checked by EXPLAIN. | `seams.md` with the plan pastes. | **PROVEN** | `task/wo/ice-tsns-floor-doors-1/evidence/seams.md`: N1 the RePark floor-plus-zone kernel, N2 the write-side Arrow cast, N3 the store-assignment cast; INSERT-family EXPLAIN pasted; MERGE EXPLAIN refused by the router and DataFrame writers have no SQL EXPLAIN, so those doors rest on code plus values. pins: ice-tsns-floor-doors-1/C-005 |
| C-006 | There is one seam where every door's narrowing is decided, by target column type. | The sketch owed before step 1. | **OPEN** | Q1: step 0 counts three distinct narrowing points (floor-plus-zone kernel, write-side Arrow cast, store-assignment cast), and the overwrite EXPLAIN displays N1 while storing N2/N3 values. The order halts on more than one seam. |
| C-007 | Step 0 measures only the listed surface and changes no product file. | The diff and the not-measured list of §4. | **PROVEN** | §4; `git diff --stat origin/main HEAD -- 'crates/*/src' python/repark/src` prints nothing. pins: ice-tsns-floor-doors-1/C-007 |
| C-008 | The build's fixture derives every expected tick from the recorder's rule and is red first on main's release module. | `test_ice_tsns_floor_doors_1.py` run on main's module, with a tick count per door and target. | **PROVEN** | §6: 180 of 246 tests red on main; 4400 of 26928 ticks red, 1976 `TIMESTAMP` and 2424 `TIMESTAMP_NTZ`, every one from a nanosecond source; the step 0 subset is red on 712 ticks (304 and 408), as the sketch counted. pins: ice-tsns-floor-doors-1/C-008 |

## 1. The matrix (C-001)

Zones UTC, America/New_York, Asia/Kolkata. Eleven ticks: -1, -1001, -999, 0,
1, 999, 1001 ns; the New York repeated-hour and skipped-hour walls read as
UTC nanoseconds; int64 minimum and maximum nanosecond instants. Sources
`timestamp_ns` and `timestamptz_ns` from a version-3 source table; targets
`TIMESTAMP` and `TIMESTAMP_NTZ` on versions 2 and 3; nine doors (INSERT,
INSERT OVERWRITE, MERGE insert arm, MERGE update arm, UPDATE from a sibling
column, `overwritePartitions`, `insertInto(overwrite=True)`,
`writeTo().append()`, `insertInto` append). References are derived by the
recorder from the rule (floor `tick // 1000`, cross-domain conversion through
`zoneinfo`); the oracle is ruling Q-21c-8, since Spark cannot read the source
types. Recorded on the release module of main at `018d0efd`.

## 2. The counts (C-002, C-003, C-004)

TIMESTAMP, both versions identically: the five append-class doors 66 of 66
EQUAL (UPDATE on v2 refuses instead, §4); the four overwrite doors 28 EQUAL,
16 truncated, 14 wall-as-UTC, 8 both. TIMESTAMP_NTZ, both versions
identically: every answering door 42 EQUAL, 21 truncated, 3 other. The 72
registry cells reproduce as the 48 collapsed combinations with the split
holding on all of them.

## 3. The seams (C-005, C-006)

Three distinct narrowing points: the RePark kernel (floor plus session zone),
the write-side Arrow cast (toward zero, no zone step), and the
store-assignment DataFusion cast (toward zero, no zone step). The overwrite
doors display the kernel in EXPLAIN and store the cast values, so the seam
lies downstream of the displayed projection. The sketch must say more than
one seam, and the unit halts for the owner under the order's section 6.

## 4. What was not measured (C-007)

No product file changed; no existing test file changed. Not measured: nested
leaves (R-011, R-015); the frame door (R-019); `applyInPandas` (R-020); the
create door; nanosecond targets; MERGE spelling variants beyond the single
insert and update arms; the cost gate; any planner head (U-1 lands first and
the build re-measures the 72 control cells there).

## 6. The fixture, red first (C-008)

`python/repark/tests/test_ice_tsns_floor_doors_1.py`, committed failing. Every
expected tick is `rule_expected` of the step 0 recorder: the floor by `//`,
the naive wall localised in the session zone and the instant shifted to the
session wall through `zoneinfo`. A `TIMESTAMP_NTZ` or `TIMESTAMP` source runs
through the same rule on its nanosecond twin (`micros * 1000`). Run on main's
release module (built from `293896ce`; main has moved by docs and tests
only since):

| Group | Tests | Red tests | Ticks | Red ticks | Expected v2 refusals |
|---|---|---|---|---|---|
| Step 0 doors, nanosecond sources | 108 | 75 | 2376 | 712 (304 `TIMESTAMP`, 408 `TIMESTAMP_NTZ`) | 132 ticks |
| Added doors x four layouts, nanosecond sources | 132 | 102 | 11088 | 3688 (1672, 2016) | 0 |
| Microsecond sources, every door and layout | (same tests) | | 13464 | 0 | 0 |
| Read casts (`TIMESTAMP_NTZ` on its own name; `TIMESTAMP` control) | 6 | 3 (`TIMESTAMP_NTZ`) | | | |
| All | 246 | 180 | 26928 | 4400 | 132 |

Red ticks by door and target, nanosecond sources, every zone, version and layout:

| Door | `TIMESTAMP` | `TIMESTAMP_NTZ` |
|---|---|---|
| INSERT, `insertInto` append, `writeTo().append()`, MERGE update arm | 0 | 48 each |
| UPDATE from a sibling column (v3; v2 refuses) | 0 | 24 |
| INSERT OVERWRITE, MERGE insert arm, `overwritePartitions`, `insertInto(overwrite=True)` | 76 each | 48 each |
| INSERT BY NAME, INSERT OVERWRITE BY NAME, INSERT OVERWRITE (k, v, id), MERGE insert `*`, MERGE insert literal | 304 each | 192 each |
| INSERT OVERWRITE `PARTITION (k = 0)` (partitioned layouts only) | 152 | 96 |
| REPLACE WHERE, `overwrite(cond)`, `saveAsTable` append, MERGE update `*`, MERGE update literal | 0 | 192 each |

H1 restated for main: every door that step 0 recorded 66 of 66 on `TIMESTAMP`
(INSERT, MERGE update arm, UPDATE on v3, `writeTo().append()`, `insertInto`
append) is green here on every tick. Not hit.

## 5. Gates

Gates run before the final hand-back per the brief; exit codes are recorded
there, not here.

## Q. Questions for a ruling

- **Q1 (C-006, OWNER).** Step 0 counts three distinct narrowing points, so
  the order's halt rule fires. Should the sketch name the single seam anyway,
  or does the unit halt here for the owner? *Premise:* the evidence above;
  the order says more than one seam halts. *Lean:* halt here; the build waits
  for the planner head regardless.

```
COVERAGE_ATTESTATION:
  pr_unit: ice-tsns-floor-doors-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Step 0 propositions walked against the order sections 0, 1, 4 step 0 and 6, registry row R-009, and ledger question Q2; the matrix and seam claims carry the evidence files named in C-001 and C-005.
      artifacts: [task/ledgers/staging/ice-tsns-floor-doors-1-ledger.md, task/wo/ice-tsns-floor-doors-1/evidence/main-cells.json]
    - id: AT-2
      status: ATTACKED
      evidence: 216 combinations over sources, targets, nine doors, two versions, three zones with eleven ticks each, read back as integer microsecond ticks with rule-derived references beside every observation.
      artifacts: [task/wo/ice-tsns-floor-doors-1/evidence/main-cells.json, task/wo/ice-tsns-floor-doors-1/evidence/main-cells.md]
    - id: AT-3
      status: N/A
      justification: Step 0 changes no product file, so there is no production code to attack for unwrap, locks, or async.
    - id: AT-4
      status: N/A
      justification: Measurement only; no shared mutable state introduced.
    - id: AT-5
      status: N/A
      justification: No credentials, no network, no secret-bearing output; the recorder uses temp warehouses by path argument.
    - id: AT-6
      status: N/A
      justification: Step 0 adds no pins; the red-first obligation belongs to the build steps.
    - id: AT-7
      status: ATTACKED
      evidence: Recorder run on the release module of main with the head pinned; ruff check and format on the recorder; the brief gate list runs before the final hand-back with exit codes recorded there.
      artifacts: [python/repark/tests/_record_ice_tsns_floor_doors_1_main.py]
    - id: AT-8
      status: ATTACKED
      evidence: The oracle is ruling Q-21c-8 (floor), since Spark 4.1.2 cannot read the source types; references are derived from the rule at record time, never typed in.
      artifacts: [task/wo/ice-tsns-floor-doors-1/evidence/main-cells.json]
    - id: AT-9
      status: N/A
      justification: Step 0 files no registry change; R-009 stays OPEN.
    - id: AT-10
      status: N/A
      justification: Step 0 changes no behaviour, so there are no behaviour-move guards to run.
  complete: true
```

VERDICT: 8 clauses, 7 PROVEN, 1 OPEN, 0 REJECTED.
